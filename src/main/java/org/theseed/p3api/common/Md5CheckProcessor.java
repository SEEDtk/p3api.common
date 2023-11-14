package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.FieldInputStream;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3Connection;
import org.theseed.utils.BasePipeProcessor;

/**
 * This subcommand reads a file of protein MD5s and loops through the genome dumps in a
 * PATRIC genome dump directory to find how many proteins are in the MD5 set.
 *
 * The MD5 file should be on the standard input.  The positional parameter is the name
 * of the genome dump directory.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing MD5s (if not STDIN)
 * -o	output file for genome report (if not STDOUT)
 * -c	index (1-based) or name of the input file column containing the MD5s.
 */
public class Md5CheckProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(Md5CheckProcessor.class);
    /** set of MD5s */
    private Set<String> md5Set;
    /** genome files to read */
    private File[] gDirs;
    /** index of input MD5 column */
    private int md5ColIdx;


    // COMMAND-LINE OPTIONS

    /** index or name of the input file column containing the MD5s */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "md5", usage = "index (1-based) or name of input file column containing the MD5s")
    private String md5Col;

    /** genome input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory containing genome dumps", required = true)
    private File inDir;

    @Override
    protected void setPipeDefaults() {
        this.md5Col = "1";
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
         if (! this.inDir.isDirectory())
             throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
         // Get the genome subdirectories.
         this.gDirs = this.inDir.listFiles(P3Connection.GENOME_FILTER);
         log.info("{} genome feature files found in {}.", this.gDirs.length, this.inDir);
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        this.md5ColIdx = inputStream.findField(this.md5Col);
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Get the MD5 set from the input stream.
        log.info("Reading MD5s from input.");
        this.md5Set = new HashSet<String>();
        for (var line : inputStream)
            this.md5Set.add(line.get(this.md5ColIdx));
        log.info("{} MD5s found in input stream.", this.md5Set.size());
        // Write the output header.
        writer.println("genome_id\tprot_found\tprot_missed");
        // These will be the global totals.
        int totalFound = 0;
        int totalMissed = 0;
        // Loop through the genomes.
        for (File gDir : gDirs) {
            final String genome_id = gDir.getName();
            log.info("Processing genome {}.", genome_id);
            File featFile = new File(gDir, P3Connection.JSON_FILE_NAME);
            try (FieldInputStream featStream = FieldInputStream.create(featFile)) {
                // Find the MD5 field for the feature file.
                int md5Idx = featStream.findField("aa_sequence_md5");
                // Loop through the features.
                int found = 0;
                int missed = 0;
                for (var line : featStream) {
                    String md5 = line.get(md5Idx);
                    if (! StringUtils.isBlank(md5)) {
                        // Here we have a valid protein.
                        if (this.md5Set.contains(md5))
                            found++;
                        else
                            missed++;
                    }
                }
                log.info("{} proteins found, {} missed in genome {}.", found, missed, genome_id);
                totalFound += found;
                totalMissed += missed;
                writer.println(genome_id + "\t" + Integer.toString(found) + "\t" + Integer.toString(missed));
            }
        }
        log.info("{} total found, {} total missed.", totalFound, totalMissed);
    }


}
