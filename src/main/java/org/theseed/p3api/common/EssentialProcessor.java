/**
 *
 */
package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.StringUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Criterion;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command runs through a list of feature IDs in PATRIC and interrogates the
 * specialty gene table to determine which of them are essential.  The list of
 * features  comes in on the standard input, which should be a tab-delimited file
 * with the genome IDs in the first column and a description in the second
 * column.  The output will contain a flag in the third column indicating which
 * of the genes are essential.
 *
 * To determine essentiality, we look for a specialty-gene record in PATRIC that
 * has a "property" value of "Essential Gene".  This will be done in batches for
 * performance.
 *
 * There are no positional parameters.  The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input feature ID list file (if not STDIN)
 * -o	output report file (if not STDOUT)
 * -b	batch size for queries (default 50)
 *
 * @author Bruce Parrello
 *
 */
public class EssentialProcessor extends BasePipeProcessor {

    //  FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(EssentialProcessor.class);
    /** connection to PATRIC */
    private P3Connection p3;
    /** current batch, mapping feature IDs to descriptions */
    private LinkedHashMap<String, String> batchMap;
    /** number of essential genes found */
    private int essentialCount;
    /** number of genes processed */
    private int geneCount;

    // COMMAND-LINE OPTION

    /** batch size for queries */
    @Option(name = "--batchSize", aliases = { "--batch", "--b" }, metaVar = "100",
            usage = "number of features to query in each batch")
    private int batchSize;

    @Override
    protected void setPipeDefaults() {
        this.batchSize =  50;
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be positive.");
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Connect to PATRIC.
        log.info("Connecting to PATRIC.");
        this.p3 = new P3Connection();
        // The current batch of features will be stored in here.
        this.batchMap = new LinkedHashMap<String, String>(this.batchSize * 2);
        // Clear the counters.
        this.essentialCount = 0;
        this.geneCount = 0;
        // Write the output header.
        String[] labels = inputStream.getLabels();
        writer.println(labels[0] + "\t" + labels[1] + "\tessential");
        // Loop through the input, filling up and processing batches.
        for (TabbedLineReader.Line line : inputStream) {
            if (this.batchMap.size() >= this.batchSize)
                this.processBatch(writer);
            this.batchMap.put(line.get(0), line.get(1));
            this.geneCount++;
        }
        // Process the residual batch.
        if (! this.batchMap.isEmpty())
            this.processBatch(writer);
        log.info("{} genes processed, {} essential.", geneCount, essentialCount);
    }

    /**
     * Query the data base to determine which features in the current batch are
     * considered essential.
     *
     * @param writer	output writer for the report
     */
    private void processBatch(PrintWriter writer) {
        log.info("Processing batch ({} features input so far).", this.geneCount);
        // Return a record for each input feature considered essential.
        var results = p3.query(Table.SP_GENE, "patric_id,property",
                Criterion.IN("patric_id", this.batchMap.keySet()));
        log.info("{} results found in query for {} genes.", results.size(),
                this.batchMap.size());
        // Form the genes found into a set.
        var essentials = results.stream()
                .filter(x -> StringUtils.equals(P3Connection.getString(x, "property"), "Essential Gene"))
                .map(x -> P3Connection.getString(x, "patric_id"))
                .collect(Collectors.toSet());
        // Output all the genes in the batch.
        for (Map.Entry<String, String> entry : this.batchMap.entrySet()) {
            String geneId = entry.getKey();
            String flag = "";
            if (essentials.contains(geneId)) {
                flag = "Y";
                this.essentialCount++;
            }
            writer.format("%s\t%s\t%s%n", geneId, this.batchMap.get(geneId), flag);
        }
        // Clear the batch for the next run.
        this.batchMap.clear();
    }

}
