package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseMultiReportProcessor;

/**
 * This subcommand is used to test the FinderRepsProcessor. It takes as input a list of file names and target repgen
 * sizes. These are passed into FinderRepsProcessor, and the output size of the resulting representative genome set
 * is compared to the target size to verify correctness. A report will be written indicating the percent error on
 * each file and the mean error overall.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file containing the list of file names and target repgen sizes (if not STDIN)
 * -D   output directory for the report files (default FKMassTest in the current directory)
 * 
 * --fuzz   fuzz factor to use in the repgen test (default 0.0, meaning none)
 * --roles  role definition file to use (default roles.for.finder in the current directory)
 */
public class FinderMassTestProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /* logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderMassTestProcessor.class);
    /** statistics for the error levels */
    private SummaryStatistics errorStats;
    /** command-line array for the FinderRepsProcessor */
    private String[] finderRepsArgs;
    /** position for the input file argument in the FinderRepsProcessor command-line array */
    private static final int INPUT_POS = 1;
    /** position for the output directory argument in the FinderRepsProcessor command-line array */
    private static final int OUTPUT_POS = 3;
    /** position for the target size argument in the FinderRepsProcessor command-line array */
    private static final int TARGET_POS = 5;

    // COMMAND-LINE OPTIONS

    /** fuzz factor for the repgen test */
    @Option(name = "--fuzz", metaVar = "0.25", usage = "fuzz factor for the repgen tests")
    private double fuzz;

    /** role definition file to use */
    @Option(name = "--roles", metaVar = "roles.for.finder", usage = "role definition file to use (default roles.for.finder in the current directory)")
    private File rolesFile;

    /** input file for the test specifications (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "repTests.tbl", usage = "input file for the test specifications (if not STDIN)")
    private File inputFile;

    @Override
    protected File setDefaultOutputDir(File curDir) {
        File defDir = new File(curDir, "FKMassTest");
        return defDir;
    }

    @Override
    protected void setMultiReportDefaults() {
        this.fuzz = 0.0; // default fuzz factor
        this.inputFile = null; // default input file is STDIN
        this.rolesFile = new File("roles.for.finder").getAbsoluteFile(); // default role definition file
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        if (this.fuzz < 0.0)
            throw new ParseFailureException("Fuzz factor cannot be negative.");
        if (this.inputFile != null && ! this.inputFile.canRead())
            throw new ParseFailureException("Input file " + this.inputFile + " is not found or unreadable.");
        if (this.rolesFile != null && ! this.rolesFile.canRead())
            throw new ParseFailureException("Role definition file " + this.rolesFile + " is not found or unreadable.");
    }

    @Override
    protected void runMultiReports() throws Exception {
        // Initialize the summary statistics for the error levels.
        this.errorStats = new SummaryStatistics();
        // Set up the command-line array for the FinderRepsProcessors.
        this.finderRepsArgs = new String[] { "-i", "", "-o", "", "--target", "200", "--fuzz", Double.toString(this.fuzz), "--roles", this.rolesFile.getAbsolutePath() };
        // Open the input file for reading the test specifications and the output file for the error report.
        try (TabbedLineReader inStream = this.openInput(); PrintWriter outStream = this.openReport("results.tbl")) {
            // Write the header for the results table.
            outStream.println("testFile\ttargetSize\tactualSize\terror");
            int lineCount = 0;
            for (var line : inStream) {
                lineCount++;
                File testFile = new File(line.get(0));
                int targetSize = line.getInt(1);
                if (! testFile.canRead())
                    log.error("Skipping unreadable test file: {}", testFile);
                else {
                    int actualSize = this.computeRepgen(testFile, targetSize);
                    double error = Math.abs(actualSize - targetSize) / (double) targetSize;
                    this.errorStats.addValue(error);
                    outStream.println(testFile + "\t" + targetSize + "\t" + actualSize + "\t" + error);
                    outStream.flush();
                    log.info("Processed test file #{} ({}) with target size {} and actual size {}.", lineCount, testFile, targetSize, actualSize);
                }
            }
            try (PrintWriter statsStream = this.openReport("errorStats.tbl")) {
                statsStream.println("count\t" + this.errorStats.getN());
                statsStream.println("mean error\t" + this.errorStats.getMean());
                statsStream.println("stdev\t" + this.errorStats.getStandardDeviation());
                statsStream.println("min\t" + this.errorStats.getMin());
                statsStream.println("max\t" + this.errorStats.getMax());
            }
        }
    }

    /**
     * This method runs the repgen utility and computes the actual size of the target from the output file.
     * 
     * @param testFile  the test file to process, containing the genome IDs in its first column
     * @param targetSize  the target size for the repgen computation
     * 
     * @return the actual size of the target computed from the repgen output
     * 
     * @throws Exception 
     */
    private int computeRepgen(File testFile, int targetSize) throws Exception {
        // Compute the output file name.
        String sizeString = Integer.toString(targetSize);
        String testNameBase = StringUtils.substringBeforeLast(testFile.getName(), ".");
        File outputFile = this.getOutFile(testNameBase + "_" + sizeString + ".tbl");
        // Set up the parameters for the FinderRepsProcessor command line.
        this.finderRepsArgs[OUTPUT_POS] = outputFile.getAbsolutePath();
        this.finderRepsArgs[INPUT_POS] = testFile.getAbsolutePath();
        this.finderRepsArgs[TARGET_POS] = Integer.toString(targetSize);
        // Create the processor object and run it.
        FinderRepsProcessor processor = new FinderRepsProcessor();
        processor.parseCommandLine(this.finderRepsArgs);
        processor.runCommandLine();
        // Compute the actual size from the output file.
        int retVal = TabbedLineReader.readSet(outputFile, "1").size();
        return retVal;
    }

    /**
     * Open the desired input stream and return it.
     *
     * @return the input stream for the test specifications
     * @throws IOException if the input file cannot be opened
     */
    private TabbedLineReader openInput() throws IOException {
        TabbedLineReader retVal;
        if (this.inputFile == null)
            retVal = new TabbedLineReader(System.in);
        else
            retVal = new TabbedLineReader(this.inputFile);
        return retVal;
    }

}
