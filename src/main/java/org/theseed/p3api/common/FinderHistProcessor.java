package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.proteins.RoleMap;
import org.theseed.sequence.seeds.FinderKmerConsumer;

/**
 * This command takes as input one or more genome IDs files and creates an output spreadsheet that can be used
 * to graph the distribution of finder-kmer closeness values. The output is a tab-delimited file. Each column
 * corresponds to an input genome ID file, and each row corresponds to a histogram bin of finder-kmer closeness values.
 * The first row contains the column headers, which are the names of the input genome ID files. The first column
 * contains the histogram bin labels.
 * 
 * The closeness values are all from 0 (far away) to 1 (nearly identical), so the histogram labels are completely
 * determined by the number of bins.
 * 
 * The positional parameters are the names of the input files containing the genome IDs. These are all tab-delimited
 * with headers, and the genome IDs should be in the same column in each file (whether by name or index). The command-
 * line options are as follows.
 * 
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file name (if not STDOUT)
 * -b   batch size for processing input groups (default 300)
 * -c   index (1-based) or name for column containing genome IDs in every input file (default "1")
 *
 * --bins       number of bins to use (default 50)
 * --roles      role definition file for the roles to use; default "roles.for.finder" in the current directory
 * 
 * FinderHistProcessor
 */
public class FinderHistProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderHistProcessor.class);
    /** data connection for accessing the BV-BRC database */
    private P3CursorConnection p3;
    /** map of file names to histogram bins */
    private Map<String, int[]> histMap;
    /** histogram bin upper limits (inclusive) */
    private double[] binLimits;
    /** role definition map */
    private RoleMap roleMap;
    /** number of values placed in bins */
    private int processCount;
    /** number of values out of range */
    private int outOfRangeCount;

    // COMMAND-LINE OPTIONS

    /** batch size for processing input groups */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "400", usage = "batch size for processing input groups")
    private int batchSize;

    /** index (1-based) or name for column containing genome IDs in every input file */
    @Option(name = "--column", aliases = { "-c" }, metaVar = "1", usage = "index (1-based) or name for column containing genome IDs in every input file")
    private String inColumn;

    /** number of bins to use in the histogram */
    @Option(name = "--bins", metaVar = "100", usage = "number of bins to use in the histogram")
    private int numBins;
    
    /** role definition file for the roles to use */
    @Option(name = "--roles", metaVar = "roles.for.finder", usage = "role definition file for the roles to use")
    private File roleFile;

    /** input file names */
    @Argument(index = 0, metaVar = "inFile1.tbl inFile2.tbl ...", usage = "input file names")
    private List<File> inputFiles;

    @Override
    protected void setReporterDefaults() {
        this.batchSize = 300;
        this.inColumn = "1";
        this.numBins = 50;
        this.roleFile = new File(System.getProperty("user.dir"), "roles.for.finder");
        this.inputFiles = new ArrayList<>();
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        if (this.batchSize < 10)
            throw new ParseFailureException("Batch size must be at least 10.");
        if (this.numBins <= 0)
            throw new ParseFailureException("Number of bins must be at least 1.");
        if (!this.roleFile.exists())
            throw new ParseFailureException("Role definition file not found: " + this.roleFile);
        if (this.inputFiles.isEmpty())
            throw new ParseFailureException("No input files specified.");
        // Load the role file into the role map.
        this.roleMap = RoleMap.load(this.roleFile);
        log.info("{} role definitions loaded from {}.", this.roleMap.size(), this.roleFile);
        // Set up the histogram bin limits.
        this.binLimits = new double[this.numBins];
        for (int i = 0; i < this.numBins; i++)
            this.binLimits[i] = (i + 1) / (double) this.numBins;
        // In case there's a tiny rounding error, ensure the last bin limit is exactly 1.0.
        this.binLimits[this.numBins - 1] = 1.0;
        log.info("Histogram bin limits set up with {} bins.", this.numBins);
        // Create the histogram map. We use a tree map because we expect the number of files to be small.
        this.histMap = new TreeMap<>();
        // Connect to the database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Remember the number of input files.
        final int numFiles = this.inputFiles.size();
        // Now loop through the input files, processing each one in turn.
        for (File inputFile : this.inputFiles) {
            log.info("Processing input file {} of {}: {}", this.inputFiles.indexOf(inputFile) + 1, numFiles, inputFile);
            // Use the input file base name as the key in the histogram map.
            String baseName = inputFile.getName();
            final int[] hist = new int[this.numBins];
            this.histMap.put(baseName, hist);
            // Get all the genome IDs in the input file.
            Set<String> genomeIds = TabbedLineReader.readSet(inputFile, this.inColumn);
            // Clear the counters.
            this.processCount = 0;
            this.outOfRangeCount = 0;
            // Compute the genome distances to fill the histogram.
            FinderKmerConsumer.process(genomeIds.stream(), FinderKmerConsumer.SamplingType.DENSE, batchSize, p3, roleMap,
                x -> this.placeInBin(x, hist), baseName + " histogram");
            log.info("Finished processing input file {}. Placed {} values, {} out of range.", baseName, this.processCount, this.outOfRangeCount);
        }
        // Now we have all the histograms filled, so we write the report. Start with the header line.
        log.info("Producing histogram report.");
        writer.print("File");
        for (File inFile : this.inputFiles)
            writer.print("\t" + inFile.getName());
        writer.println();
        // Now we loop through the bins. For each bin, we write the label and then the counts for each input file.
        for (int binIndex = 0; binIndex < this.numBins; binIndex++) {
            writer.print(this.binLimits[binIndex]);
            for (File inFile : this.inputFiles) {
                int[] hist = this.histMap.get(inFile.getName());
                writer.print("\t" + hist[binIndex]);
            }
            writer.println();
        }
    }

    /**
     * Place a value in the appropriate histogram bin.
     * 
     * @param value	the value to place in the histogram
     * @param hist	the histogram array
     */
    private void placeInBin(double value, int[] hist) {
        int i = 0;
        if (value < 0.0 || value > 1.0) {
            this.outOfRangeCount++;
        } else {
            final int N = this.numBins - 1;
            while (i < N && value > this.binLimits[i]) i++;
            hist[i]++;
            this.processCount++;
        }
    }

}
