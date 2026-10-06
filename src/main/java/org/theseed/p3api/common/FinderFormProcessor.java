package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * FinderFormProcessor
 *
 * This processor computes formula coefficients from the results of finderSamp. It takes as input the
 * summary.tbl file from the finder-sampling run and computes the coefficients of a power formula (y = ax^b)
 * for the output size based on the closeness limits. The ultimate goal is to compute a formula
 * for the coefficients from the mean, standard deviation, and original sample size.
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
 */
public class FinderFormProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderFormProcessor.class);
    /** index of first data column in the summary.tbl file */
    private double[] closenessLimits;
    /** array of label column headers */
    private String[] labelHeaders;

    @Override
    protected void setPipeDefaults() {
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
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the header line to the output. The header consists of the label columns followed by the coefficients and an error measure.
        writer.print(String.join("\t", this.labelHeaders));
        writer.print("\ta\tb\terror");
        writer.println();
        // Loop through the input file, processing each line.
        int lineCounter = 0;
        log.info("Processing input file.");
        for (var line : inputStream) {
            lineCounter++;
            // Get the values of the various labels.
            String[] labelValues = new String[this.labelHeaders.length];
            for (int i = 0; i < this.labelHeaders.length; i++)
                labelValues[i] = line.get(i);
            writer.print(String.join("\t", labelValues));
            // Now we need to compute the coefficients. We build a regression model. The y-value is the
            // number in the current line, and the x-value is the corresponding closeness limit.
            SimpleRegression regression = new SimpleRegression();
            for (int i = 0; i < this.closenessLimits.length; i++) {
                double y = line.getDouble(this.labelHeaders.length + i);
                double x = this.closenessLimits[i];
                regression.addData(Math.log(x), Math.log(y));
            }
            double b = regression.getSlope();
            double a = Math.exp(regression.getIntercept());
            // Now we compute the mean error.
            double error = 0.0;
            for (int i = 0; i < this.closenessLimits.length; i++) {
                double y = line.getDouble(this.labelHeaders.length + i);
                double x = this.closenessLimits[i];
                double yPred = a * Math.pow(x, b);
                error += Math.abs(yPred - y) / y;
            }
            error /= this.closenessLimits.length;
            writer.printf("\t%.6f\t%.6f\t%.6f%n", a, b, error);
            if (lineCounter % 1000 == 0)
                log.info("Processed {} lines.", lineCounter);
        }
        log.info("All done. {} lines processed.", lineCounter);
    }
}
