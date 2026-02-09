package org.theseed.p3api.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.Jsoner;

/**
 * This command combines multiple JSON files into a single file. The input files should all match a pattern specified in the command-line options, and will
 * be taken from subdirectories of a specified input directory. The output file will be written to the standard output. There is no guarantee of the order
 * in which the input files will be read.
 * 
 * Each record from one of the input files will be a record in the output file. It is presumed every incoming JSON file is a list rather than a single object.
 * If a file is a single object, it is treated as a list of one object. There will be no pretty formatting, and each file must be able to fit in memory, so
 * this is not suitable for sets with very large individual files.
 * 
 * The positional parameter is the name of the input directory. The command-line options are as follows.
 * 
 * -h       display command-line usage
 * -v       show more detailed log messages
 * -o       output file (if not STDOUT)
 * 
 * --pattern    the file name pattern to search for (default "*.json")
 * 
 */

public class CombineJsonProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(CombineJsonProcessor.class);
    /** list of subdirectories to process */
    private File[] subDirs;
    /** file name filter */
    private FileFilter patternFilter;

    // COMMAND-LINE OPTIONS

    /** file name pattern to search for */
    @Option(name = "--pattern", metaVar = "cds*.json",usage = "file name pattern to search for")
    private String pattern;

    /** input directory name */
    @Argument(index = 0, metaVar = "inputDir", usage = "input directory name", required = true)
    private File inputDir;

    @Override
    protected void setReporterDefaults() {
        this.pattern = "*.json";
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Create the file filter. This will fail if the pattern is invalid.
        this.patternFilter = (FileFilter) new WildcardFileFilter.Builder().setWildcards(this.pattern).get();
        // Validate the input directory.
        if (! this.inputDir.isDirectory())
            throw new IOException("Input directory " + this.inputDir + " is not found or invalid.");
        // Get the list of subdirectories. We will only process files in these subdirectories.
        this.subDirs = this.inputDir.listFiles(File::isDirectory);
        log.info("{} subdirectories found in {}.", this.subDirs.length, this.inputDir);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Write out the initial left bracket for the JSON list.
        writer.println("[");
        // Loop through the subdirectories. We will accumulate all the output files into a single
        // file name list.
        List<File> jsonFileList = new ArrayList<>();
        for (File subDir : this.subDirs) {
            // Get the list of files in this subdirectory that match the pattern.
            File[] jsonFiles = subDir.listFiles(this.patternFilter);
            log.info("{} files found in {}.", jsonFiles.length, subDir);
            jsonFileList.addAll(Arrays.asList(jsonFiles));
        }
        log.info("{} total files found in {} subdirectories.", jsonFileList.size(), this.subDirs.length);
        // We will count the output records in here.
        int outCount = 0;
        // This will count the number of files processed.
        int fileCount = 0;
        // No delimiter is needed before the first record of the first file. We have the bracket.
        boolean delimiterNeeded = false;
        // Loop through the files. The last file is a special case because we don't want to write a comma after the last record.
        for (int i = 0; i < jsonFileList.size(); i++) {
            File jsonFile = jsonFileList.get(i);
            fileCount++;
            // Read the json File.
            Object jsonFileData;
            try (FileReader reader = new FileReader(jsonFile)) {
                jsonFileData = Jsoner.deserialize(reader);
            }
            if (jsonFileData instanceof JsonArray jsonList) {
                log.info("{} records found in file {} of {}: {}.", jsonList.size(), fileCount, jsonFileList.size(), jsonFile);
                // This is already a list, so we can just write out each record.
                for (Object record : jsonList) {
                    // Finish the previous record with a comma if needed, then write the new record.
                    if (delimiterNeeded)
                        writer.println(",");
                    else
                        delimiterNeeded = true;
                    writer.print(Jsoner.serialize(record));
                    outCount++;
                }
            } else {
                log.info("Single object found in file {} of {}: {}.", fileCount, jsonFileList.size(), jsonFile);
                // This is a single object, so we write it as a single record. Finish the previous record with a comma if needed, 
                // then write the whole record.
                if (delimiterNeeded)
                    writer.println(",");
                else
                    delimiterNeeded = true;
                writer.print(Jsoner.serialize(jsonFileData));
                outCount++;
            }
        }
        // Write out the closing right bracket for the JSON list.
        writer.println("]");
        log.info("{} total records written from {} files.", outCount, fileCount);
    }

}
