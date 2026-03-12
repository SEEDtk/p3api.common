package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.LineReader;

/**
 * This command looks at a run list shell command to start cluster jobs during MCQ generation and removes the
 * completed groups from the run list. This is used to keep the run list up to date as jobs complete.
 * 
 * A log file summary is used to determine which groups are completed. The group name is the last parameter of each
 * line in the run list. It also appears as the name of the output json file without the ".json" suffix. If the
 * log file contains a line ending in a json file name, the corresponding group is removed from the run list.
 * 
 * The run list comes in on the standard input and the updated run list is written to the standard output. The log file 
 * is specified as the sole positional parameter.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file (if not STDIN)
 * -o   output file (if not STDOUT)
 * 
 */
public class DoneCheckProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(DoneCheckProcessor.class);
    /** map of job group names to run lines */
    private Map<String, String> runMap;
    /** pattern to match for log file lines; first group is question count, second group is group name */
    private static final Pattern LOG_PATTERN = Pattern.compile(".+\\s+(\\d+) good questions.+\\/(\\S+)\\.json\\..*");
    
    // COMMAND-LINE OPTIONS

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "runScript.sh", usage = "input file (if not STDIN)")
    private File inputFile;

    /** log file to check for completed groups */
    @Argument(index = 0, metaVar = "logFile.log", usage = "log file to check for completed groups", required = true)
    private File logFile;

    @Override
    protected void setReporterDefaults() {
        // Default to standard input for the script file.
        this.inputFile = null;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Here we validate the log file and read the run list into a map.
        if (! this.logFile.canRead())
            throw new FileNotFoundException("Log file " + this.logFile + " is not found or unreadable.");
        try (LineReader inStream = this.openInput(this.inputFile)) {
            // Read the run list into a map of group name to run line.
            this.runMap = new HashMap<>();
            for (String line : inStream) {
                String group = StringUtils.substringAfterLast(line, " ");
                this.runMap.put(group, line);
            }
            log.info("{} groups found in input script.", this.runMap.size());
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Now we read the log file and remove any groups that are completed. A group is completed if the log file contains a line ending in the group name with a ".json" suffix.
        try (LineReader logStream = new LineReader(this.logFile)) {
            int skipped = 0;
            int questions = 0;
            for (String line : logStream) {
                Matcher matcher = LOG_PATTERN.matcher(line);
                if (matcher.matches()) {
                    int count = Integer.parseInt(matcher.group(1));
                    questions += count;
                    String group = matcher.group(2);
                    if (this.runMap.containsKey(group)) {
                        this.runMap.remove(group);
                        log.info("Group {} is completed and removed from the run list.", group);
                    } else
                        skipped++;
                }
            }
            log.info("{} groups in the log file were not found in the run list. {} total questions completed.", skipped, questions);
        }
        // Write the remaining run lines to the output.
        for (String line : this.runMap.values())
            writer.println(line);
    }

    /**
     * This method opens the input file for the run script. It defaults to standard input if no file is specified.
     * 
     * @param inputFile     input file name, or NULL if we are to use STDIN
     * 
     * @return a LineReader for the input stream
     * 
     * @throws IOException 
     */
    private LineReader openInput(File inputFile) throws IOException {
        LineReader retVal;
        if (inputFile == null) {
            log.info("Input will be read from the standard input.");
            retVal = new LineReader(System.in);
        } else if (! inputFile.canRead())
            throw new RuntimeException("Input file " + inputFile + " is not found or unreadable.");
        else {
            log.info("Input will be read from {}.", inputFile);
            retVal = new LineReader(inputFile);
        }
        return retVal;
    }


}
