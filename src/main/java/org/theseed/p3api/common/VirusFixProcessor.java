/**
 *
 */
package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.utils.BasePipeProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command processes a tab-delimited file containing assembly accession IDs and assertions about
 * viruses.  It then interrogates the PATRIC database to find the genome ID, length, name, contig
 * sequence ID, and contig sequence MD5 of each virus output for that accession ID. If will flag
 * duplicate MD5s when found.
 *
 * The standard input should be a tab-delimited file containing assembly accession IDs in the first
 * column and assertions in the second. The standard output will be have the necessary additional
 * columns.
 *
 * Note that the query batch size is the number of assembly accession IDs in a batch. The batch size
 * will grow because most accession IDs have 2 or more genomes attached. Thus, this batch size should
 * be kept lower than normal.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file (if not STDIN)
 * -o	output file (if not STDOUT)
 * -b	batch size for PATRIC queries (default 100)
 *
 * @author Bruce Parrello
 *
 */
public class VirusFixProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(VirusFixProcessor.class);
    /** PATRIC data connection */
    private P3Connection p3;
    /** accession ID column index */
    private int accIdColIdx;
    /** assertion column index */
    private int assertColIdx;
    /** set of MD5s already encountered */
    private Set<String> dupCheckSet;
    /** map of accession IDs to assertions for the current batch */
    private Map<String, String> accessionBatch;
    /** map of genome IDs to genome descriptors for the current batch */
    private Map<String, GenomeData> genomeBatch;
    /** number of output lines written */
    private int outCount;
    /** number of duplicates found */
    private int dupCount;
    /** number of input lines read */
    private int lineCount;
    /** number of batches processed */
    private int batchCount;
    /** number of error results returned */
    private int errCount;

    /**
     * This object contains the data we need for a genome.
     */
    protected class GenomeData {

        /** genome ID */
        private String id;
        /** genome name */
        private String name;
        /** length in base pairs */
        private int size;
        /** assertion */
        private String assertString;
        /** accession ID */
        private String accessionId;

        /**
         * Create a genome descriptor from the data record returned from PATRIC.
         *
         * @param genome	data record to process
         */
        public GenomeData(JsonObject genome) {
            this.id = P3Connection.getString(genome, "genome_id");
            this.name = P3Connection.getString(genome, "genome_name");
            this.size = P3Connection.getInt(genome, "genome_length");
            this.accessionId = P3Connection.getString(genome, "assembly_accession");
            this.assertString = VirusFixProcessor.this.accessionBatch.getOrDefault(this.accessionId, "Bad");
        }

        /**
         * @return the genome ID
         */
        public String getId() {
            return this.id;
        }

        /**
         * @return the genome name
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return the size in base pairs of the virus
         */
        public int getSize() {
            return this.size;
        }

        /**
         * @return the assertion text
         */
        public String getAssertString() {
            return this.assertString;
        }

        /**
         * @return the assembly accession ID
         */
        public String getAccessionId() {
            return this.accessionId;
        }

    }

    // COMMAND-LINE OPTIONS

    /** batch size for queries */
    @Option(name = "--batchSize", aliases = { "--batch", "-b" }, metaVar = "50", usage = "batch size for PATRIC queries")
    private int batchSize;

    @Override
    protected void setPipeDefaults() {
        this.batchSize = 100;
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        // Connect to PATRIC.
        log.info("Connecting to PATRIC.");
        this.p3 = new P3Connection();
        // Initialize the duplicate-check set.
        this.dupCheckSet = new HashSet<String>(2000);
        // Create the batch hashes.
        this.accessionBatch = new HashMap<String, String>(this.batchSize * 4 / 3 + 10);
        this.genomeBatch = new HashMap<String, GenomeData>(this.batchSize * 4 + 1);
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Get the input column indices.
        this.accIdColIdx = inputStream.findField("assembly_accession");
        this.assertColIdx = inputStream.findField("assertion");
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("genome_id\tlength\tassertion\tgenome_name\tassembly_accession\tcontig.sequence_id\tcontig.sequence_md5\tdup");
        // Set up counters.
        this.lineCount = 0;
        this.batchCount = 0;
        this.outCount = 0;
        this.dupCount = 0;
        this.errCount = 0;
        // Loop through the input, creating batches to process.
        for (var line : inputStream) {
            // Insure there is room for another accession ID.
            if (this.accessionBatch.size() >= this.batchSize) {
                // Process the current batch.
                this.processBatch(writer);
                // Clear the batch data for the next pass.
                this.genomeBatch.clear();
                this.accessionBatch.clear();
            }
            // Store this record in the batch map.
            String accId = line.get(this.accIdColIdx);
            String assertion = line.get(this.assertColIdx);
            this.accessionBatch.put(accId, assertion);
            this.lineCount++;
        }
        // Process the residual batch.
        if (! this.accessionBatch.isEmpty())
            this.processBatch(writer);
        log.info("{} total lines read, {} output, {} duplicates found, {} errors.", this.lineCount,
                this.outCount, this.dupCount, this.errCount);
    }

    /**
     * Process a batch of accession IDs.  We get the genome data for each, then use a second query to
     * ask for the sequence data. This is used to check for duplicates. Lastly, we write the data
     * accumulated.
     *
     * @param writer	output print writer
     */
    private void processBatch(PrintWriter writer) {
        this.batchCount++;
        log.info("Processing batch {} with {} accession IDs. {} lines read so far.",
                this.batchCount, this.lineCount, this.outCount);
        // Retrieve the genomes for these accession IDs.
        List<JsonObject> genomes = this.p3.getRecords(Table.GENOME, "assembly_accession", this.accessionBatch.keySet(),
                "assembly_accession,genome_id,genome_name,genome_length");
        log.info("{} genomes returned from query for {} accession IDs.", genomes.size(), this.accessionBatch.size());
        // Create descriptors for the genomes found.
        for (JsonObject genome : genomes) {
            GenomeData desc = this.new GenomeData(genome);
            this.genomeBatch.put(desc.getId(), desc);
        }
        // Now ask for the sequence data. Because these are viruses there is at most one sequence per genome.
        List<JsonObject> contigs = this.p3.getRecords(Table.CONTIG, "genome_id", this.genomeBatch.keySet(),
                "genome_id,sequence_id,sequence_md5");
        log.info("{} sequences returned from query for {} genome IDs.", contigs.size(), this.genomeBatch.size());
        // Check for duplicates and output the full records found.
        for (JsonObject contig : contigs) {
            String genomeId = P3Connection.getString(contig, "genome_id");
            GenomeData desc = this.genomeBatch.get(genomeId);
            if (desc == null) {
                log.error("Invalid genome ID {} returned from contig query.", genomeId);
                this.errCount++;
            } else {
                String seqId = P3Connection.getString(contig, "sequence_id");
                String seqMd5 = P3Connection.getString(contig, "sequence_md5");
                String dupFlag;
                if (this.dupCheckSet.contains(seqMd5)) {
                    // Here we have a duplicate.
                    this.dupCount++;
                    dupFlag = "Y";
                } else {
                    dupFlag = "";
                    this.dupCheckSet.add(seqMd5);
                }
                // Here we can output a record.
                writer.println(genomeId + "\t" + desc.getSize() + "\t" + desc.getAssertString() + "\t"
                        + desc.getName() + "\t" + desc.getAccessionId() + "\t" + seqId + "\t"
                        + seqMd5 + "\t" + dupFlag);
                this.outCount++;
            }
        }
    }

}
