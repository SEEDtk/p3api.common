package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * FinderTableProcessor
 *
 * This processor creates a table of finderSamp results. It takes as input the
 * summary.tbl file from the finder-sampling run and produces a tab-delimited table with one row per result.
 * The table will be constructed with the inputs being the label columns, the output set size, and one of
 * the output column set sizes. The output column will be the closeness limit. The table can then be analyzed
 * to find a formula for computing the closeness limit based on the sample characteristics and the desired
 * output set size.
 * 
 * The output report will be written to the standard output. The summary.tbl file should be on the standard
 * input.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file name (if not STDIN)
 * -o   output file name (if not STDOUT)
 * 
 * --testLimit   specify a closeness limit output to be used as input (default 0.8)
 */
public class FinderTableProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderTableProcessor.class);
    /** index of first data column in the summary.tbl file */
    private double[] closenessLimits;
    /** array of label column headers */
    private String[] labelHeaders;
    /** column index for the test limit */
    private int testLimitIdx;

    // COMMAND-LINE OPTIONS

    /** closeness limit to use for an input column */
    @Option(name = "--testLimit", usage = "specify a closeness limit output to be used as input (default 0.8)")
    private String testLimit;

    @Override
    protected void setPipeDefaults() {
        this.testLimit = "0.8";
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // The input file consists of a set of label columns followed by closeness limit columns.
        // We separate the two parts here.
        String[] headers = inputStream.getLabels();
        List<String> headerList = new ArrayList<>();
        int idx = 0;
        while (idx < headers.length && ! headers[idx].matches("\\d+(\\.\\d+)?")) {
            headerList.add(headers[idx]);
            idx++;
        }
        if (idx == headers.length)
            throw new IOException("No closeness limits found in header line.");
        this.labelHeaders = headerList.toArray(String[]::new);
        this.closenessLimits = new double[headers.length - idx];
        for (int i = idx; i < headers.length; i++)
            this.closenessLimits[i - idx] = Double.parseDouble(headers[i]);
        // Finally, save the index of the test limit column. This will throw an exception
        // if the column spec is invalid.
        this.testLimitIdx = inputStream.findField(this.testLimit);
        // Insure that the test limit index is within the closeness limits range.
        if (this.testLimitIdx < this.labelHeaders.length)
            throw new IOException("Test limit column is not a result column.");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {

    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the header line to the output. The header consists of the label columns followed by the test column,
        // the output size, and the closeness limit.
        writer.print(String.join("\t", this.labelHeaders));
        writer.print("\tsample\tx\ty");
        writer.println();
        // Loop through the input file, processing each line.
        int lineCounter = 0;
        int outCounter = 0;
        log.info("Processing input file.");
        for (var line : inputStream) {
            lineCounter++;
            // Get the values of the various labels.
            String[] labelValues = new String[this.labelHeaders.length];
            for (int i = 0; i < this.labelHeaders.length; i++)
                labelValues[i] = line.get(i);
            // Get the value of the test column.
            String testValue = line.get(this.testLimitIdx);
            // Form all of these into a string. This will be the prefix for each output line from
            // this input line.
            String prefix = String.join("\t", labelValues) + "\t" + testValue + "\t";
            // For each closeness limit, we will extract the output size (x) and then write this followed
            // by the limit itself (y).
            for (int i = 0; i < this.closenessLimits.length; i++) {
                double x = line.getInt(this.labelHeaders.length + i);
                double y = this.closenessLimits[i];
                writer.println(prefix + x + "\t" + y);
                outCounter++;
            }
            if (outCounter % 1000 == 0)
                log.info("Output {} lines.", outCounter);
        }
        log.info("All done. {} lines processed, {} written.", lineCounter, outCounter);
    }
}
