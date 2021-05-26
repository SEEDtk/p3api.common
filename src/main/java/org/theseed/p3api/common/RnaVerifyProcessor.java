/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Connection;
import org.theseed.p3api.Connection.Table;
import org.theseed.sequence.DnaKmers;
import org.theseed.p3api.Criterion;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command runs through the good genomes in PATRIC and computes the kmer distance between eligible
 * SSU rRNA features.  An RNA feature is eligible if it is at least the minimum permissible length.
 *
 * The pgenome list will be taken from the standard input, which must be a tab-delimited file. The genomes will be
 * processed in batches for performance.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	specify output file (if not STDIN)
 * -c	index (1-based) or name of input file column containing genome IDs (default "1")
 * -b	genome batch size (default 100)
 * -m	minimum acceptable SSU rRNA length
 * -K	DNA kmer size
 * -i	input file (if not STDIN)
 *
 * @author Bruce Parrello
 *
 */
public class RnaVerifyProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaVerifyProcessor.class);
    /** input file reader */
    private TabbedLineReader inStream;
    /** genome ID column index */
    private int colIdx;
    /** current batch of genome IDs to process */
    private Set<String> genomeIds;
    /** connection to PATRIC */
    private Connection p3;
    /** number of batches processed */
    private int batchCount;
    /** number of genomes output */
    private int genomesOut;
    /** number of genomes skipped */
    private int skipCount;
    /** number of sequences missing */
    private int missCount;
    /** number of genomes processed */
    private int processCount;
    /** start time */
    private long start;

    // COMMAND-LINE OPTIONS

    /** input column name or index */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of the genome ID input column")
    private String column;

    /** query batch size */
    @Option(name = "--batchSize", aliases = { "--batch", "-b" }, metaVar = "10", usage = "batch size for PATRIC queries")
    private int batchSize;

    /** minimum acceptable SSU rRNA length */
    @Option(name = "--minLen", aliases = { "--min", "-m" }, metaVar = "1200", usage = "minimum acceptable RNA length")
    private int minLen;

    /** DNA kmer size */
    @Option(name = "--kmer", aliases = { "-K" }, metaVar = "12", usage = "DNA kmer size")
    private int kmerSize;

    /** input file name */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "genomes.tbl", usage = "input file with genome IDs")
    private File inFile;

    @Override
    protected void setReporterDefaults() {
        this.column = "1";
        this.batchSize = 100;
        this.minLen = 1400;
        this.kmerSize = 15;
        this.inFile = null;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        if (this.kmerSize < 3)
            throw new ParseFailureException("Kmer size must be at least 3.");
        if (this.minLen < this.kmerSize)
            throw new ParseFailureException("Minimum RNA length must be at least equal to kmer size.");
        // Set the kmer size.
        DnaKmers.setKmerSize(this.kmerSize);
        log.info("DNA kmer size is {}.", this.kmerSize);
        // Check the input file.
        if (this.inFile == null) {
            log.info("Genome IDs will be read from standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("Genome IDs will be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        this.colIdx = this.inStream.findField(this.column);
        // Create the genome batch holder.
        this.genomeIds = new HashSet<String>(this.batchSize);
        // Connect  to PATRIC.
        log.info("Connecting to PATRIC.");
        this.p3 = new Connection();
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        try {
            this.batchCount = 0;
            this.skipCount = 0;
            this.missCount = 0;
            this.genomesOut = 0;
            this.processCount = 0;
            // Write the output header.
            writer.println("genome_id\tgenome_name\trna_count\tmax_dist");
            this.start = System.currentTimeMillis();
            // Loop through the genome list in the input.
            for (TabbedLineReader.Line line : this.inStream) {
                // Get the genome ID.
                String genomeId = line.get(this.colIdx);
                // Insure there is room for it.
                if (this.genomeIds.size() >= this.batchSize)
                    this.processBatch(writer);
                this.genomeIds.add(genomeId);
            }
            // Process the residual.
            if (this.genomeIds.size() > 0)
                this.processBatch(writer);
            // Write the stats.
            log.info("{} genomes processed in {} batches.  {} bad RNAs, {} genomes had too few RNAs.",
                    this.genomesOut, this.batchCount, this.missCount, this.skipCount);
        } finally {
            this.inStream.close();
        }
    }

    /**
     * Process a batch of genomes.  We must read in the RNAs for the genomes and then check the distances.
     *
     * @param writer	output writer for the report
     */
    private void processBatch(PrintWriter writer) {
        this.batchCount++;
        log.info("Processing batch {}: {} genomes.", this.batchCount, this.genomeIds.size());
        // This hash will hold the RNA MD5s for each genome.
        Map<String, List<String>> rnaMap = new HashMap<String, List<String>>(this.batchSize);
        // This hash will hold the name of each genome.
        Map<String, String> nameMap = new HashMap<String, String>(this.batchSize);
        // Run the RNA query.
        List<JsonObject> rnaRecords = this.p3.getRecords(Table.FEATURE, "genome_id", this.genomeIds, "genome_id,genome_name,product,na_sequence_md5",
                Criterion.EQ("feature_type", "rrna"), Criterion.GE("na_length", this.minLen));
        log.info("{} rRNA records found in batch {}.", rnaRecords.size(), this.batchCount);
        // Sort the RNAs, saving the sequence MD5s.
        Set<String> rnaSeqIds = new HashSet<String>(rnaRecords.size());
        for (JsonObject rnaRecord : rnaRecords) {
            String function = Connection.getString(rnaRecord, "product");
            if (Genome.SSU_R_RNA.matcher(function).find()) {
                // Save the MD5 so we can query the sequence.
                String seqId = Connection.getString(rnaRecord, "na_sequence_md5");
                rnaSeqIds.add(seqId);
                // Save the genome name.
                String genomeId = Connection.getString(rnaRecord, "genome_id");
                String genomeName = Connection.getString(rnaRecord, "genome_name");
                nameMap.put(genomeId, genomeName);
                // Connect the MD5 to the genome.
                List<String> rnaSeqs = rnaMap.computeIfAbsent(genomeId, x -> new ArrayList<String>(10));
                rnaSeqs.add(seqId);
            }
        }
        log.info("{} RNA sequences found in {} genomes.", rnaSeqIds.size(), nameMap.size());
        // Now retrieve the sequences.
        Map<String, JsonObject> seqRecords = this.p3.getRecords(Table.SEQUENCE, rnaSeqIds, "sequence");
        log.info("{} sequences returned.", seqRecords.size());
        // Finally, process each genome.
        for (String genomeId : this.genomeIds) {
            List<String> rnaList = rnaMap.get(genomeId);
            if (rnaList == null) {
                log.warn("No useful RNAs found for {}.", genomeId);
                this.skipCount++;
            } else {
                // Loop through the RNAs, comparing the sequences.
                List<DnaKmers> kmers = new ArrayList<DnaKmers>(rnaList.size());
                for (String seqId : rnaList) {
                    if (! seqRecords.containsKey(seqId)) {
                        log.warn("Could not find DNA for genome {} sequence {}.", genomeId, seqId);
                        this.missCount++;
                    } else {
                        String seq = Connection.getString(seqRecords.get(seqId), "sequence");
                        if (seq.length() < this.minLen) {
                            log.warn("DNA for genome {} sequence {} has improper length.", genomeId, seqId);
                            missCount++;
                        } else {
                            kmers.add(new DnaKmers(seq));
                        }
                    }
                }
                // Insure we have at least 2 RNAs.
                if (kmers.size() < 2) {
                    log.warn("Genome {} has too few useful RNAs to test.", genomeId);
                    this.skipCount++;
                } else {
                    double maxDist = 0.0;
                    for (int i = 0; i < kmers.size() && maxDist < 1.0; i++) {
                        DnaKmers kmersI = kmers.get(i);
                        for (int j = i + 1; j < kmers.size(); j++) {
                            double dist = kmersI.distance(kmers.get(j));
                            if (dist > maxDist)
                                maxDist = dist;
                        }
                    }
                    // Now we have our result.
                    writer.format("%s\t%s\t%d\t%6.4f%n", genomeId, nameMap.get(genomeId), kmers.size(), maxDist);
                    this.genomesOut++;
                }
            }
        }
        // Clear the batch for next time.
        this.processCount += this.genomeIds.size();
        this.genomeIds.clear();
        log.info("{} genomes processed in {} seconds.", this.processCount, (System.currentTimeMillis() - start) / 1000L);
    }

}
