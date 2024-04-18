/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.basic.BaseReportProcessor;

/**
 * This is a simple program that runs through the files in the subdirectories of a master directory.
 * All files in different directories with the same base name will be compared, and the directory
 * containing the largest file with each name will be output.
 *
 * The positional parameter is the name of the input master directory.
 *
 * The report will be written to the standard output.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for report (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class FindBigFileProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FindBigFileProcessor.class);
    /** master hash of file names */
    private Map<String, FileData> bigFileMap;
    /** array of subdirectories to check */
    private File[] subDirs;
    /** file filter for finding subdirectories */
    private FileFilter SUB_DIR_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };
    /** file filter for finding readable files */
    private FileFilter READABLE_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.canRead();
        }
    };

    // COMMAND-LINE OPTIONS

    @Argument(index = 0, metaVar = "inDir", usage = "input master directory to scan", required = true)
    private File inDir;

    /**
     * This object contains the data about the largest file with a given name.
     *
     * @author Bruce Parrello
     *
     */
    protected static class FileData {

        /** directory containing the file */
        private File parentDir;
        /** size of file */
        private long size;

        /**
         * Construct a new, blank file data object.
         */
        protected FileData() {
            this.parentDir = null;
            this.size = 0L;
        }

        /**
         * Merge in a new file if it's bigger.
         *
         * @param other		new file to check
         */
        public void merge(File other) {
            long otherLen = other.length();
            if (otherLen > this.size) {
                this.parentDir = other.getParentFile();
                this.size = otherLen;
            }
        }

        /**
         * @return the parent directory name
         */
        public File getParentDir() {
            return this.parentDir;
        }

        /**
         * @return the size of the file
         */
        public long getSize() {
            return this.size;
        }

    }

    @Override
    protected void setReporterDefaults() {
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Verify that the input directory is valid.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        // Get the list of subdirectories to scan.
        this.subDirs = this.inDir.listFiles(SUB_DIR_FILTER);
        log.info("{} subdirectories found in {}.", this.subDirs.length, this.inDir);
        // Create the master hash.
        this.bigFileMap = new TreeMap<String, FileData>();
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Loop through the sub-directories.
        for (File subDir : this.subDirs) {
            // Get the files in this directory.
            File[] subFiles = subDir.listFiles(READABLE_FILTER);
            log.info("{} files to scan in {}.", subFiles.length, subDir);
            for (File subFile : subFiles) {
                String name = subFile.getName();
                FileData fileDatum = this.bigFileMap.computeIfAbsent(name, x -> new FileData());
                fileDatum.merge(subFile);
            }
        }
        // Now we have all the files we need in the big file map.  Write the report.
        writer.println("file_name\tdirectory\tsize");
        for (var fileEntry : this.bigFileMap.entrySet()) {
            String fileName = fileEntry.getKey();
            FileData fileDatum = fileEntry.getValue();
            writer.println(fileName + "\t" + fileDatum.getParentDir() + "\t" + fileDatum.getSize());
        }
    }

}
