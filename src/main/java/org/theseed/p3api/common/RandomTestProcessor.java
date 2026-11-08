package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.LineReader;
import org.theseed.stats.Shuffler;
import org.theseed.utils.BaseMultiReportProcessor;

/**
 * This is a very specialized but useful subcommand that generates control groups for repgen tests.
 * It accepts as input two directories: one containing genome group files and one containing
 * representative samples with similar names. It then generates randomly-selected control groups
 * of the same size as the representative groups for comparison purposes.
 * 
 * The files are all tab-delimited, with headers, possessing one genome per line. For each file
 * in the full-group directory, we find the corresponding file in the representative-sample directory,
 * count the lines, and then randomly select that many genomes from the full group to go in the
 * output directory.
 * 
 * The positional parameters are the names of the full-group directory and the representative-sample directory.
 * In the default case, each full-group file ends in ".tbl", and each representative-sample file ends in
 * ".repgen.tbl" with the same name prefix; however, this can be overridden by specifying the suffixes explicitly.
 * The output directory defaults to "Random" in the current directory, but this can be overridden as well.
 * The output files will have the same name as the full-group files with ".random" prefixed to the extension
 * and will be placed in the output directory.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -D   output directory for the control group files (default is "Random" in the current directory)
 * 
 * --full   suffix for full-group files (default is ".tbl")
 * --rep    suffix for representative-sample files (default is ".repgen.tbl")
 * --rand   suffix for output files (default is ".random.tbl")
 * --clear  erase the output directory before starting (default is false)
 * 
 */
public class RandomTestProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RandomTestProcessor.class);
    /** Map of full-group input files to their corresponding representative-sample and output files */
    private Map<File, FilePair> fileMap;

    // COMMAND-LINE OPTIONS
    /** suffix for full-group files */
    @Option(name = "--full", metaVar = ".tsv",usage = "suffix for full-group files")
    private String fullSuffix;
    
    /** suffix for output files */
    @Option(name = "--rand", metaVar = ".random.tbl", usage = "suffix for output files")
    private String randSuffix;

    /** suffix for representative-sample files */
    @Option(name = "--rep", metaVar = ".test.tbl", usage = "suffix for representative-sample files")
    private String repSuffix;

    /** input directory for full-group files */
    @Argument(index = 0, metaVar = "fullDir", usage = "input directory for full-group files", required = true)
    private File fullDir;

    /** input directory for representative-sample files */
    @Argument(index = 1, metaVar = "repDir", usage = "input directory for representative-sample files", required = true)
    private File repDir;

    @Override
    protected File setDefaultOutputDir(File curDir) {
        return new File(curDir, "Random");
    }

    /** simple record to hold a pair of file names-- the representative-sample file and the output file */
    protected static record FilePair(File repFile, File outFile) {}

    @Override
    protected void setMultiReportDefaults() {
        this.fullSuffix = ".tbl";
        this.repSuffix = ".repgen.tbl";
        this.randSuffix = ".random.tbl";
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        // Validate the two input directories.
        if (! this.fullDir.isDirectory())
            throw new IOException("Full-group input directory is invalid or not found: " + this.fullDir);
        if (! this.repDir.isDirectory())
            throw new IOException("Representative-sample input directory is invalid or not found: " + this.repDir);
        // Now we need to build the file map from the three suffixes.
        this.fileMap = new TreeMap<>();
        File[] fullFiles = this.fullDir.listFiles((dir, name) -> name.endsWith(this.fullSuffix));
        if (fullFiles == null)
            throw new FileNotFoundException("No full-group files found in directory: " + this.fullDir);
        int missingCount = 0;
        for (File fullFile : fullFiles) {
            String baseName = fullFile.getName().substring(0, fullFile.getName().length() - this.fullSuffix.length());
            File repFile = new File(this.repDir, baseName + this.repSuffix);
            File outFile = this.getOutFile(baseName + this.randSuffix);
            if (repFile.isFile())
                this.fileMap.put(fullFile, new FilePair(repFile, outFile));
            else
                missingCount++;
        }
        log.info("{} full-group files ready for processing, {} missing representative-sample files.", 
                this.fileMap.size(), missingCount);
    }

    @Override
    protected void runMultiReports() throws Exception {
        // This will track the number of processed files.
        int processedCount = 0;
        // Keep the total number of files to process.
        final int totalFiles = this.fileMap.size();
        // Loop through the file map and process each pair of files.
        for (Map.Entry<File, FilePair> entry : this.fileMap.entrySet()) {
            File fullFile = entry.getKey();
            FilePair pair = entry.getValue();
            File repFile = pair.repFile();
            File outFile = pair.outFile();
            // Count this file.
            processedCount++;
            log.info("Processing file {}/{}: {}", processedCount, totalFiles, fullFile);
            // Count the data lines in the representative-sample file.
            int lineCount = (int) Files.lines(repFile.toPath()).count() - 1;
            if (lineCount <= 0)
                throw new IOException("Representative-sample file " + repFile + " has no data lines.");
            else {
                log.info("Representative-sample file {} has {} lines.", repFile, lineCount);
                int estimatedFullLines = (int) (fullFile.length() / (repFile.length() / lineCount));
                // Read the lines from the full file.
                try (LineReader fullReader = new LineReader(fullFile)) {
                    // Load the header.
                    String header = fullReader.next();
                    // Load the data lines from the full file.
                    Shuffler<String> dataLines = new Shuffler<>(estimatedFullLines);
                    while (fullReader.hasNext()) {
                        String line = fullReader.next();
                        dataLines.add(line);
                    }
                    log.info("Full file {} has {} data lines (estimated {}).", fullFile, dataLines.size(), estimatedFullLines);
                    // Now select a random subset of the data lines.
                    dataLines.shuffle(lineCount);
                    // Write them to the output file.
                    try (PrintWriter writer = new PrintWriter(outFile)) {
                        writer.println(header);
                        for (int i = 0; i < lineCount; i++) {
                            String line = dataLines.get(i);
                            writer.println(line);
                        }
                    }
                    log.info("Output file {} written with {} data lines.", outFile, lineCount);
                }
            }
        }
    }


    


}
