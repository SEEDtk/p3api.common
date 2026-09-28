package org.theseed.p3api.common;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.proteins.RoleMap;
import org.theseed.sequence.seeds.FinderKmerBatch;
import org.theseed.sequence.seeds.FinderKmerStats;
import org.theseed.utils.BaseMultiReportProcessor;
import org.theseed.utils.FloatList;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command is a stress test for the FinderKmer representative sets. It takes as input a set of sample files. For each file
 * it generates statistics for the genome closeness measures, then tests various representative sets based on multiples of the
 * standard deviation from the mean. The multiples to be tried are specified on the command line.
 * 
 * The positional parameters are the names of the input files containing the genome IDs. Wildcards are allowed. A report will be generated 
 * for each repgen set as well as a master report for all the sets. All reports will go in the output directory as specified in the command-line
 * options.
 * 
 * Multiple repgen sets can be generated in a single pass. This greatly speeds up the processing but it can cause a great strain on the
 * memory usage. Therefore, by default, the sets are generated no more than 4 at a time.
 * 
 * The command-line options are as follows:
 * 
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -D	output directory name; default "FKreports" in the current directory
 * -b   batch size for processing input groups (default 400)
 * -c   index (1-based) or name for column containing genome IDs in every input file (default "1")
 * -N   maximum number of repgen sets to generate in per pass (default 4)
 *
 * --clear	    erase the output directory before processing
 * --limits     closeness limits for the representative sets, comma-delimited, must be between 0 and 1 (default "0.2,0.4,0.5,0.6,0.7,0.8,0.9,0.95")
 * --roles      role definition file for the roles to use; default "roles.for.finder" in the current directory
 *
 * @author Bruce Parrello
 * 
 */
public class FinderSampProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /** logging facility */
    protected static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderSampProcessor.class);
    /** list of limit multipliers for the representative sets */
    private double[] limits;
    /** role definition map */
    private RoleMap roleMap;
    /** BV-BRC database connection */
    private P3CursorConnection p3;
    /** corrected list of input files */
    private List<File> inputFiles;

    // COMMAND-LINE OPTIONS

    /** batch size for processing input groups */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "300", usage = "batch size for processing input groups")
    private int batchSize;

    /** index (1-based) or name for column containing genome IDs in every input file */
    @Option(name = "--column", aliases = { "-c" }, metaVar = "1", usage = "index (1-based) or name for column containing genome IDs in every input file")
    private String inColumn;
    
    /** closeness limits */
    @Option(name = "--limits", metaVar = "0.2,0.4,0.6,0.8", usage = "closeness limits for the representative sets, comma-delimited")
    private String limitsString;

    /** role definition file for the roles to use */
    @Option(name = "--roles", metaVar = "roles.for.finder", usage = "role definition file for the roles to use")
    private File roleFile;
    
    /** maximum number of repgen sets to generate per pass */
    @Option(name = "--N", aliases = { "-N" }, metaVar = "4", usage = "maximum number of repgen sets to generate per pass")
    private int maxThreads;

    /** input file names (may contain wildcards) */
    @Argument(index = 0, metaVar = "inFile1.tbl inFile2.tbl ...", usage = "input file names (wildcards allowed)")
    private List<File> userInputFiles;

    @Override
    protected File setDefaultOutputDir(File curDir) {
        return new File(curDir, "FKreports");
    }

    @Override
    protected void setMultiReportDefaults() {
        this.limitsString = "0.2,0.4,0.5,0.6,0.7,0.8,0.9,0.95";
        this.userInputFiles = new ArrayList<>();
        this.roleFile = new File(System.getProperty("user.dir"), "roles.for.finder");
        this.batchSize = 400;
        this.maxThreads = 4;
        this.inColumn = "1";
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        // Insure we have input files.
        if (this.userInputFiles.isEmpty())
            throw new ParseFailureException("No input files specified.");
        // Process the wildcards.
        this.inputFiles = new ArrayList<>();
        for (File userFile : this.userInputFiles) {
            File dir = userFile.getParentFile();
            if (dir == null)
                dir = new File(System.getProperty("user.dir"));
            FileFilter myFilter = WildcardFileFilter.builder().setWildcards(userFile.getName()).get();
            File[] matchedFiles = dir.listFiles(myFilter);
            if (matchedFiles != null)
                this.inputFiles.addAll(Arrays.asList(matchedFiles));
        }
        log.info("{} input files matched after wildcard processing.", this.inputFiles.size());
        // Verify all the input files are readable.
        for (File inputFile : this.inputFiles) {
            if (!inputFile.canRead())
                throw new ParseFailureException("Input file " + inputFile + " is not found or is unreadable.");
        }
        // Verify the role file is readable.
        if (!this.roleFile.canRead())
            throw new ParseFailureException("Role file " + this.roleFile + " is not found or is unreadable.");
        // Validate the batch size.
        if (this.batchSize <= 10)
            throw new ParseFailureException("Batch size must be at least 10.");
        // Parse the limit multipliers.
        FloatList parsedLimits = new FloatList(this.limitsString);
        this.limits = new double[parsedLimits.size()];
        for (int i = 0; i < parsedLimits.size(); i++)
            this.limits[i] = parsedLimits.get(i);
        log.info("{} limit multipliers specified.", this.limits.length);
        // Read in the role map.
        this.roleMap = RoleMap.load(this.roleFile);
        log.info("Role map loaded with {} roles.", this.roleMap.size());
        // Connect to the database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runMultiReports() throws Exception {
        // Set up the main report writer.
        try (PrintWriter mainReportWriter = this.openReport("summary.tbl")) {
            // Create the header for the main report.
            mainReportWriter.print("in_file\tsize\tmin\tmean\tmax\tsdev\tskew");
            for (int idx = 0; idx < this.limits.length; idx++)
                mainReportWriter.print("\t" + this.limits[idx]);
            mainReportWriter.println();
            // We need to sort the files from smallest to largest. The hope is to get as many files
            // finished as possible before we run out of memory.
            this.inputFiles.sort((f1, f2) -> Long.compare(f1.length(), f2.length()));
            // Set up some timing.
            long startTime = System.currentTimeMillis();
            int filesProcessed = 0;
            // Now we loop through the input files.
            for (File inputFile : this.inputFiles) {
                // We will use the input file name for the report.
                String inputFileName = inputFile.getName();
                // Our last preparation step: get a map from genome IDs to names for the inputs.
                // We will need the names when we output the repgen file.
                Map<String, String> genomeMap = this.getGenomeMap(inputFile);
                // Now we compute the stats for these genomes.
                FinderKmerStats stats = FinderKmerStats.compute(genomeMap.keySet().stream(), FinderKmerStats.SamplingType.DENSE,
                        this.batchSize, this.p3, this.roleMap);
                // Get the min, max, mean, standard deviation, and skewness from the stats.                
                double min = stats.getMin();
                double max = stats.getMax();
                double mean = stats.getMean();
                double sdev = stats.getStdDev();
                double skew = stats.getSkewness();
                log.info("Computed statistics for {} genomes in input file {}. Mean = {} and stdev = {} with skew {}. Min/Max = {}/{}.", 
                    genomeMap.size(), inputFileName, mean, sdev, skew, min, max);
                // Put the statistics in the output report line.
                StringBuilder reportLine = new StringBuilder(100);
                reportLine.append(inputFileName).append('\t').append(genomeMap.size()).append('\t').append(min).append('\t')
                        .append(mean).append('\t').append(max).append('\t').append(sdev).append('\t').append(skew);
                for (int groupIdx = 0; groupIdx < this.limits.length; groupIdx += this.maxThreads) {
                    int endIdx = Math.min(groupIdx + this.maxThreads, this.limits.length);
                    // Process the group of limits from groupIdx to endIdx - 1.
                    FinderKmerBatch[] repgenSetsGroup = FinderKmerBatch.createRepresentativeSubsets(genomeMap.keySet().stream(), this.p3, this.roleMap, 
                            this.batchSize, Arrays.copyOfRange(this.limits, groupIdx, endIdx));
                    // Now we need to write each representative set to an output file and finish the report line for this input genome set.
                    for (int idx = 0; idx < repgenSetsGroup.length; idx++) {
                        double limit = this.limits[groupIdx + idx];
                        // Update the output line for the master report.
                        FinderKmerBatch repgenSet = repgenSetsGroup[idx];
                        reportLine.append('\t').append(repgenSet.size());
                        // Write the repgen set to an output file. The output file name consists of the main name of the input file with the limit 
                        // appended.
                        String outFileName = StringUtils.substringBeforeLast(inputFileName, ".") + "_" + limit + ".tbl";
                        try (PrintWriter repgenWriter = this.openReport(outFileName)) {
                            repgenWriter.println("genome_id\tgenome_name");
                            for (String genomeId : repgenSet.genomeIds())
                                repgenWriter.println(genomeId + "\t" + genomeMap.get(genomeId));
                            log.info("{} genomes in representative set for limit {} written to {}", repgenSet.size(), limit, outFileName);
                        }
                    }
                }
                // Write the master report line.
                mainReportWriter.println(reportLine.toString());
                mainReportWriter.flush();
                filesProcessed++;
                if (log.isInfoEnabled()) {
                    int filesLeft = this.inputFiles.size() - filesProcessed;
                    long timeSpent = System.currentTimeMillis() - startTime;
                    if (filesLeft > 0) {
                        long millisPerFile = timeSpent / filesProcessed + 1;
                        Duration eta = Duration.ofMillis(millisPerFile * filesLeft);
                        log.info("{} files left to process. Estimated time remaining: {}.", filesLeft, eta);
                    } else {
                        log.info("{} files processed in {}.", filesProcessed, Duration.ofMillis(timeSpent));
                    }
                }
            }
        }
    }

    /**
     * This method reads the genome IDs from the input file and then finds the names in the database.
     * It forms these into a map from IDs to names for the genomes to process.
     * 
     * @param inputFile         name of the input file containing genome IDs
     * 
     * @return                  a map from genome IDs to genome names
     * 
     * @throws IOException
     */
    private Map<String, String> getGenomeMap(File inputFile) throws IOException {
        // Get the set of genome IDs from the designated column.
        Set<String> genomes = TabbedLineReader.readSet(inputFile, this.inColumn);
        log.info("{} genomes found in input file {}", genomes.size(), inputFile.getAbsolutePath());
        // Now ask the database for the names corresponding to these genome IDs.
        List<JsonObject> records = this.p3.getRecords("genome", P3CursorConnection.MAX_LIMIT, this.batchSize, 
                "genome_id", genomes, "genome_id,genome_name");
        // Now we loop through the results and build the map.
        Map<String, String> retVal = new HashMap<>(records.size() * 3 / 2);
        for (JsonObject record : records) {
            String genomeId = KeyBuffer.getString(record, "genome_id");
            String genomeName = KeyBuffer.getString(record, "genome_name");
            retVal.put(genomeId, genomeName);
        }
        log.info("{} of {} genomes from input file {} found in database.", retVal.size(), genomes.size(), inputFile.getName());
        return retVal;
    }
    
}