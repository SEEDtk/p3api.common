package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Feature;
import org.theseed.io.TabbedLineReader;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.sequence.Sequence;
import org.theseed.sequence.seeds.ProteinFinder;
import org.theseed.utils.BasePipeProcessor;


/**
 * This is a simple command that takes as input a protein finder and a table of genome IDs and generates a testing file for the finder kmers.
 * The testing file contains all the DNA FASTA entries for the genomes listed in the input table as taken from the finder, but the
 * comment is changed from the function string to the role ID.
 * 
 * The positional parameter is the name of the input table containing genome IDs. The genome ID list is taken from the standard input, and
 * the FASTA file will be written to the standard output.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file containing genome IDs (if not STDIN)
 * -o   output FASTA file (if not STDOUT)
 * -c   index (1-based) or name of the input column containing the genome IDs (default "1")
 * 
 * @author Bruce Parrello
 */
public class FinderSampProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FinderSampProcessor.class);
    /** set of genome IDs to include in the output */
    private Set<String> genomeIds;
    /** index of the input column containing the genome IDs */
    private int idColIdx;
    /** protein finder manager */
    private ProteinFinder finder;

    // COMMAND-LINE PARAMETERS

    /** index (1-based) or name of the input column containing the genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of the input column containing the genome IDs")
    private String idColOption;

    /** name of the directory containing the protein finder */
    @Argument(index = 0, metaVar = "FinderDir", usage = "name of the directory containing the protein finder")
    private File finderDir;

    @Override
    protected void setPipeDefaults() {
        this.idColOption = "1"; // default to the first column
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Find the index of the key input column. If the input file is invalid, this will fail.
        this.idColIdx = inputStream.findField(this.idColOption);
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the protein finder.
        if (! this.finderDir.isDirectory())
            throw new IOException("Finder directory " + this.finderDir + " is not found or invalid.");
        log.info("Initializing protein finder from {}.", this.finderDir);
        this.finder = new ProteinFinder(this.finderDir);
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // First, we read the genome IDs from the input stream and store them in the genomeIds set.
        log.info("Reading genome IDs from input stream.");
        this.genomeIds = inputStream.stream().map(line -> line.get(this.idColIdx)).collect(Collectors.toSet());
        log.info("Read {} genome IDs.", this.genomeIds.size());
        // Now, we open the output writer as a FASTA file. This is where we'll put the output.
        try (FastaOutputStream fastaWriter = new FastaOutputStream(writer)) {
            // Set up some counters.
            int seqsIn = 0;
            int seqsOut = 0;
            int rolesIn = 0;
            // Loop through the role files and process each one.
            Map<String, File> roleFileMap = this.finder.getFastas();
            for (Map.Entry<String, File> entry : roleFileMap.entrySet()) {
                String role = entry.getKey();
                File fastaFile = entry.getValue();
                rolesIn++;
                log.info("Processing role #{} {} from file {}.", rolesIn, role, fastaFile);
                long lastMsg = System.currentTimeMillis();
                try (FastaInputStream fastaReader = new FastaInputStream(fastaFile)) {
                    for (Sequence seqIn : fastaReader) {
                        seqsIn++;
                        String genomeId = Feature.genomeOf(seqIn.getLabel());
                        if (this.genomeIds.contains(genomeId)) {
                            seqIn.setComment(role);
                            fastaWriter.write(seqIn);
                            seqsOut++;
                        }
                        long now = System.currentTimeMillis();
                        if (now - lastMsg > 10000) {
                            log.info("Processed {} sequences. Role #{} {} in progress. {} sequences written.", seqsIn, rolesIn, role, seqsOut);
                            lastMsg = now;
                        }
                    }
                }
            }

            log.info("{} sequences read, {} written, {} roles processed.", seqsIn, seqsOut, rolesIn);
        }
    }

}