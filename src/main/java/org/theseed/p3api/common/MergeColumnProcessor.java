/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * This is a simple utility that takes as input a single-column file and then merges in
 * values from a chosen column of a second file.  The input is on the standard input and
 * the output on the standard output.
 *
 * The secondary input is de-duplicated but the primary input is not.
 *
 * The positional parameter is the name of the second file.  Both it and the input file
 * should be tab-delimited with headers.  The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	primary input file (if not STDIN)
 * -o	primary output file (if not STDOUT)
 * -c	index (1-based) or name of the input column in the secondary input file (default "1")
 *
 * @author Bruce Parrello
 *
 */
public class MergeColumnProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MergeColumnProcessor.class);
    /** set of values from the secondary input */
    private Set<String> newValues;
    /** field name for output file */
    private String colName;

    // COMMAND-LINE OPTIONS

    /** input column specifier for secondary input file */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "id", usage = "index (1-based) or name of input column in secondary file")
    private String inCol;

    /** name of the secondary input file */
    @Argument(index = 0, metaVar = "inFile2", usage = "name of the secondary input file", required = true)
    private File inFile2;

    @Override
    protected void setPipeDefaults() {
        this.inCol = "1";
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Insure the secondary input file is valid.
        if (! this.inFile2.canRead())
            throw new FileNotFoundException("Secondary input file " + this.inFile2 + " is not found or unreadable.");
        // Read in the secondary values.
        log.info("Reading new values from column {} of {}.", this.inCol, this.inFile2);
        this.newValues = TabbedLineReader.readSet(this.inFile2, this.inCol);
        log.info("{} new values read from input.", this.newValues.size());
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Here we just need to know that the input file is single-column.
        if (inputStream.size() != 1)
            throw new IOException("Input must be single-column.");
        // Save the field name.
        this.colName = inputStream.getLabels()[0];
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        log.info("Coping input to output.");
        // Write the output header.
        writer.println(this.colName);
        // Set up some counters.
        int inLines = 0;
        int outLines = 0;
        int skipLines = 0;
        // Loop through the primary input.
        for (var line : inputStream) {
            inLines++;
            String oldValue = line.get(0);
            // Write the old value if it is not redundant.
            if (! this.newValues.contains(oldValue)) {
                writer.println(oldValue);
                outLines++;
            } else
                skipLines++;
        }
        // Spool off the new values.
        for (String newValue : this.newValues) {
            writer.println(newValue);
            outLines++;
        }
        log.info("{} lines read from primary.  {} skipped, {} total lines written.", inLines, skipLines,
                outLines);
    }

}
