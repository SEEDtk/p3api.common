package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.kohsuke.args4j.Argument;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;

/**
 * This is a very special-purpose command for cleaning duplicates genomes from AMR test files. The test files
 * are two-column files where the first column is the genome ID and the second column is the genome name. Each
 * file has a name of the form XXXXXXX.suscept.tbl or XXXXXXX.resist.tbl. Files with almost the same name are
 * paired, and we need to remove genomes that appear in both files of each pair. The files are loaded into memory
 * and then saved back over the top of the original file. For this reason, the input directory should be backed
 * up before running this command.
 * 
 * The file formats are very strict. The order of the records does not matter, each file has the same header line,
 * and each line is two fields, tab-delimited.
 * 
 * The single positional parameter is the name of the input directory containing the AMR test files.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 */
public class TestCleanProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestCleanProcessor.class);
    /** map of resist file names to suscept file names */
    private Map<String, String> inputFileMap;
    /** header to use for all the files */
    private String[] header;

    // COMMAND-LINE OPTIONS

    /** name of the input directory */
    @Argument(index = 0, metaVar = "inputDir", usage = "input directory containing AMR test files")
    private File inputDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected void validateParms() throws IOException, ParseFailureException {
        // Verify that the input directory exists and is a directory
        if (! inputDir.isDirectory())
            throw new IOException("Input directory " + inputDir + " is not found or invalid.");
        // Get the list of files in the input directory.
        File[] files = inputDir.listFiles();
        if (files == null)
            throw new IOException("Failed to list files in input directory " + inputDir);
        if (files.length == 0)
            throw new IOException("No files found in input directory " + inputDir);
        // Now we loop through the files and build the map of resist to suscept file names.
        this.inputFileMap = new HashMap<>(files.length);
        for (File file : files) {
            if (file.isFile()) {
                String name = file.getName();
                if (name.endsWith(".resist.tbl")) {
                    String base = name.substring(0, name.length() - ".resist.tbl".length());
                    String susName = base + ".suscept.tbl";
                    File susFile = new File(inputDir, susName);
                    if (susFile.isFile())
                        inputFileMap.put(name, susName);
                }
            }
        }
        log.info("{} input file pairs found.", inputFileMap.size());
        // Extract the header from the first file, if available.
        if (! inputFileMap.isEmpty()) {
            String firstResist = inputFileMap.keySet().iterator().next();
            File firstFile = new File(inputDir, firstResist);
            try (TabbedLineReader firstStream = new TabbedLineReader(firstFile)) {
                if (firstStream.size() != 2)
                    throw new IOException("Invalid file format in " + firstFile + ": expected 2 columns, found " 
                            + firstStream.size() + ".");
                this.header = firstStream.getLabels();
            }
        }
    }

    @Override
    protected void runCommand() throws Exception {
        int fileCount = 0;
        // It's important to understand that the order of the records in the input files does not matter. We
        // will load each file into a tree map, delete the common keys, and then write the maps back to disk.
        // Loop through each input file pair and process them.
        for (var fileEntry : inputFileMap.entrySet()) {
            String resistName = fileEntry.getKey();
            String susceptName = fileEntry.getValue();
            File resistFile = new File(inputDir, resistName);
            File susceptFile = new File(inputDir, susceptName);
            Map<String, String> resistMap = this.loadFile(resistFile);
            Map<String, String> susceptMap = this.loadFile(susceptFile);
            fileCount++;
            log.info("Processing file pair {}: {} and {}", fileCount, resistName, susceptName);
            // Find the set of common keys.
            Set<String> commonKeys = new HashSet<>(resistMap.keySet());
            commonKeys.retainAll(susceptMap.keySet());
            log.info("Found {} common keys.", commonKeys.size());
            // Remove the common keys from both maps.
            for (String key : commonKeys) {
                resistMap.remove(key);
                susceptMap.remove(key);
            }
            // Write the updated maps back to disk.
            this.writeFile(resistFile, resistMap);
            this.writeFile(susceptFile, susceptMap);
        }
        log.info("Processed {} file pairs.", fileCount);
    }

    /**
     * Load a file into a map of key-value pairs.
     * 
     * @param file  name of the file to load
     * @return a map of key-value pairs from the file (column 1 to column 2)
     * 
     * @throws IOException if an error occurs while reading the file
     */
    private Map<String, String> loadFile(File file) throws IOException {
        Map<String, String> retVal = new TreeMap<>();
        try (TabbedLineReader stream = new TabbedLineReader(file)) {
            if (stream.size() != 2)
                throw new IOException("Invalid file format in " + file + ": expected 2 columns, found " 
                        + stream.size() + ".");
            for (TabbedLineReader.Line line : stream) {
                String key = line.get(0);
                String value = line.get(1);
                retVal.put(key, value);
            }
            log.info("{} entries loaded from {}.", retVal.size(), file);
        }
        return retVal;
    }

    /**
     * Write a map of key-value pairs to a file.
     * 
     * @param file  the file to write to
     * @param map   the map of key-value pairs to write
     * 
     * @throws IOException if an error occurs while writing the file
     */
    private void writeFile(File file, Map<String,String> map) throws IOException {
        try (PrintWriter writer = new PrintWriter(file)) {
            // Write the header line first.
            writer.println(this.header[0] + "\t" + this.header[1]);
            // Write the data lines.
            for (Map.Entry<String, String> entry : map.entrySet())
                writer.println(entry.getKey() + "\t" + entry.getValue());
        }
        log.info("Wrote {} entries to {}.", map.size(), file);
    }


}
