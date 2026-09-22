package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.proteins.Role;
import org.theseed.proteins.RoleMap;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.sequence.Sequence;
import org.theseed.utils.BasePipeProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;


/**
 * This is a simple command that takes as input a role file and a table of genome IDs and generates a testing file for the finder kmers.
 * The testing file contains all the DNA FASTA entries for the genomes listed in the input table as taken from the finder, but the
 * comment is changed from the function string to the role ID.
 * 
 * The positional parameter is the name of the role file. The genome ID list is taken from the standard input, and
 * the FASTA file will be written to the standard output.
 * 
 * We use the roles.for.finder to compute the product value we want for each role and then do a protein search in the SOLR database. This
 * is a bit iffy, because we may miss roles that are mis-spelled. We verify everything read to insure we don't have something that is not
 * the desired role, but we can't fix the other direction. Nonetheless, this approach is much faster than reading entire genomes.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file containing genome IDs (if not STDIN)
 * -o   output FASTA file (if not STDOUT)
 * -c   index (1-based) or name of the input column containing the genome IDs (default "1")
 * -b   batch size for database queries (default 200)
 * 
 * @author Bruce Parrello
 */
public class FinderSampProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FinderSampProcessor.class);
    /** list of genome IDs to include in the output */
    private List<String> genomeIds;
    /** index of the input column containing the genome IDs */
    private int idColIdx;
    /** role map */
    private RoleMap roleMap;
    /** number of validated sequences read from the database */
    private int seqsIn;
    /** number of invalid sequences read from the database */
    private int seqsInvalid;
    /** number of sequences written to the output */
    private int seqsOut;
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;
    
    // COMMAND-LINE PARAMETERS

    /** index (1-based) or name of the input column containing the genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of the input column containing the genome IDs")
    private String idColOption;

    /** batch size for database queries */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "200", usage = "batch size for database queries (default 200)")
    private int batchSize;

    /** name of the role file */
    @Argument(index = 0, metaVar = "roles.for.finder", usage = "name of the role file")
    private File roleFile;

    @Override
    protected void setPipeDefaults() {
        this.idColOption = "1"; // default to the first column
        this.batchSize = 200; // default batch size for database queries
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Find the index of the key input column. If the input file is invalid, this will fail.
        this.idColIdx = inputStream.findField(this.idColOption);
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the role map.
        if (! this.roleFile.canRead())
            throw new IOException("Role file " + this.roleFile + " is not found or cannot be read.");
        log.info("Loading role map from {}.", this.roleFile);
        this.roleMap = RoleMap.load(this.roleFile);
        log.info("Role map loaded with {} roles.", this.roleMap.size());
        // Connect to the database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // First, we read the genome IDs from the input stream and store them in the genomeIds set.
        log.info("Reading genome IDs from input stream.");
        this.genomeIds = inputStream.stream().map(line -> line.get(this.idColIdx)).collect(Collectors.toList());
        log.info("Read {} genome IDs.", this.genomeIds.size());
        // Now, we open the output writer as a FASTA file. This is where we'll put the output.
        try (FastaOutputStream fastaWriter = new FastaOutputStream(writer)) {
            // Set up some counters.
            this.seqsIn = 0;
            this.seqsOut = 0;
            this.seqsInvalid = 0;
            int rolesIn = 0;
            // Loop through the roles and process each one.
            for (Role role : this.roleMap.objectValues()) {
                rolesIn++;
                String roleId = role.getId();
                String roleName = role.getName();
                log.info("Processing role #{} {}: {}.", rolesIn, roleId, roleName);
                // Build a filter for this role.
                SolrFilter findRole = SolrFilter.EQ("product", roleName);
                Collection<SolrFilter> criteria = Arrays.asList(findRole);
                this.p3.getRecords("feature", P3CursorConnection.MAX_LIMIT, this.batchSize, "genome_id", this.genomeIds,
                        "patric_id,genome_id,product,aa_sequence", criteria, x -> this.processRecord(x, role, fastaWriter));
                log.info("Processed {} sequences. Role #{} {} in progress. {} sequences written.", this.seqsIn, rolesIn, role, this.seqsOut);
            }
            log.info("{} sequences read, {} written, {} invalid, {} roles processed.", this.seqsIn, this.seqsOut, this.seqsInvalid, rolesIn);
        }
    }

    /**
     * This method processes a single feature record. We verify that the product matches the given role. If it does, we write the sequence
     * to the output FASTA file.
     * 
     * @param record        JSON object containing the feature record
     * @param role          descriptor for the  role being processed
     * @param fastaWriter   FASTA output stream to write the sequence to if the product matches the role
     */
    private void processRecord(JsonObject record, Role role, FastaOutputStream fastaWriter) {
        // Denote we've read another sequence.
        this.seqsIn++;
        // Validate the product against the role name.
        String product = KeyBuffer.getString(record, "product");
        if (! role.matches(product))
            this.seqsInvalid++;
        else {
            // Write the sequence to the FASTA output.
            String fid = KeyBuffer.getString(record, "patric_id");
            String protein = KeyBuffer.getString(record, "aa_sequence");
            Sequence seq = new Sequence(fid, role.getId(), protein);
            // We uncheck the IOException so we can put this method in a stream without having to declare a checked exception.
            try {
                fastaWriter.write(seq);
                this.seqsOut++;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


}