/**
 *
 */
package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.io.TabbedLineReader.Line;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.stats.QualityCountMap;
import org.theseed.utils.BasePipeProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command searches the BV-BRC database for high-quality genomes with AMR data.  It will run through
 * the genomes in the "patric.good.tbl" file, which has good genomes sorted from best quality to worst.
 * For each batch of genome IDs, it will try to find AMR records.  Any genome with at least one AMR record
 * will be output along with its score, rating, rep200 group name, and the number of susceptible and
 * resistant connections.
 *
 * The patric.good.tbl file should be on the standard input.  The report will be produced to the standard
 * output.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file with good-genome data (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -b	batch size for queries (default 200)
 *
 * --limit		maximum number of genomes to output per representative group (default 1000)
 *
 * @author Bruce Parrello
 *
 */
public class FindAmrGenomeProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FindAmrGenomeProcessor.class);
    /** index of the genome ID input column */
    private int idColIdx;
    /** index of the genome name input column */
    private int nameColIdx;
    /** index of the quality score input column */
    private int scoreColIdx;
    /** index of the rating input column */
    private int ratingColIdx;
    /** index of the rep100 group ID column */
    private int repGroupColIdx;
    /** number of batches processed */
    private int batchCount;
    /** number of genomes output */
    private int outCount;
    /** number of found genomes skipped */
    private int skipCount;
    /** connection to BV-BRC */
    private P3Connection p3;
    /** set of groups with genomes already output */
    private CountMap<String> groups;

    // COMMAND-LINE OPTIONS

    /** size of a query batch */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "100", usage = "query batch size")
    private int batchSize;

    /** maximum number of genomes to output per representative group */
    @Option(name = "--limit", metaVar = "1000000", usage = "maximum number of genomes to output per representative group")
    private int maxSize;

    @Override
    protected void setPipeDefaults() {
        this.batchSize = 200;
        this.maxSize = 1000;
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Get the input column indices.
        this.idColIdx = inputStream.findField("genome_id");
        this.nameColIdx = inputStream.findField("name");
        this.scoreColIdx = inputStream.findField("score");
        this.ratingColIdx = inputStream.findField("rating");
        this.repGroupColIdx = inputStream.findField("rep200");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Verify the batch size.
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be positive.");
        // Verify the limit.
        if (this.maxSize < 1)
            throw new ParseFailureException("Maximum per-group genome limit must be positive.");
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("genome_id\tgenome_name\tscore\trating\tresistant\tsusceptible\trep100");
        // Connect to BV-BRC.
        this.p3 = new P3Connection();
        // Initialize the counters.
        this.batchCount = 0;
        this.outCount = 0;
        this.skipCount = 0;
        int inCount = 0;
        // Set up the group selector.
        this.groups = new CountMap<String>();
        // This will hold the current query batch.
        Map<String, TabbedLineReader.Line> batch = new HashMap<String, TabbedLineReader.Line>(this.batchSize * 3 / 2 + 1);
        // Loop through the input file.  We stop when we hit the output maximum.
        var iter = inputStream.iterator();
        while (iter.hasNext()) {
            var line = iter.next();
            if (batch.size() >= this.batchSize) {
                this.processBatch(writer, batch);
                log.info("{} genomes read, {} output, {} skipped.", inCount, this.outCount, this.skipCount);
                // Set up for the next batch.
                batch.clear();
            }
            String genomeId = line.get(this.idColIdx);
            batch.put(genomeId, line);
            inCount++;
        }
        // Insure we process the residual.
        if (! batch.isEmpty())
            this.processBatch(writer, batch);
        log.info("{} total genomes read, {} output, {} skipped, {} batches submitted.", inCount, this.outCount, this.skipCount, this.batchCount);
    }

    /**
     * Process a batch of genomes.  We attempt to find AMR data, and output the genomes if any AMR records exist.
     *
     * @param writer	output print writer for results
     * @param batch		map of genome IDs to input lines for the genomes to query
     */
    private void processBatch(PrintWriter writer, Map<String, Line> batch) {
        this.batchCount++;
        log.info("Processing batch {} with {} genomes.", this.batchCount, batch.size());
        // Set up a counter for bad phenotypes.
        int badTypeCount = 0;
        // Ask for AMR data.
        List<JsonObject> results = this.p3.getRecords(Table.GENOME_AMR, "genome_id", batch.keySet(), "antibiotic,resistant_phenotype");
        if (results.isEmpty())
            log.info("No AMR records found for batch {}.", this.batchCount);
        else {
            log.info("{} AMR records found for batch {}.", results.size(), this.batchCount);
            // Loop through the results.  For each genome, we could the resistant records (good) and the susceptible records (bad).
            QualityCountMap<String> amrMap = new QualityCountMap<String>();
            for (var result : results) {
                String genomeId = KeyBuffer.getString(result, "genome_id");
                String type = KeyBuffer.getString(result, "resistant_phenotype");
                if (type.contentEquals("Resistant"))
                    amrMap.setGood(genomeId);
                else if (type.contentEquals("Susceptible"))
                    amrMap.setBad(genomeId);
                else {
                    badTypeCount++;
                }
            }
            log.info("{} eligible genomes found in batch {}.  {} bad phenotypes.", amrMap.size(), this.batchCount, badTypeCount);
            // Loop through the quality count map, writing results.
            for (String genomeId : amrMap.allKeys()) {
                var gLine = batch.get(genomeId);
                if (gLine == null)
                    log.error("Invalid genome result {} from AMR query for batch {}.", genomeId, this.batchCount);
                else {
                    // Get the group name and count this genome.
                    String group = gLine.get(this.repGroupColIdx);
                    int newCount = this.groups.count(group);
                    if (newCount > this.maxSize)
                        this.skipCount++;
                    else {
                        writer.println(genomeId + "\t" + gLine.get(this.nameColIdx) + "\t" + gLine.get(this.scoreColIdx) + "\t"
                                + gLine.get(this.ratingColIdx) + "\t" + amrMap.good(genomeId) + "\t" + amrMap.bad(genomeId) + "\t"
                                + gLine.get(this.repGroupColIdx));
                        this.outCount++;
                    }
                }
            }
            // Insure the data is output.
            writer.flush();
        }
    }

}
