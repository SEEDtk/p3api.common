/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.utils.BaseReportProcessor;

/**
 * This is a simple test utility that reads a genome source to identify bad files.  The positional parameter
 * is a genome source.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for the report (if not STDOUT)
 * -t	type of genome source (default MASTER)
 *
 * @author Bruce Parrello
 *
 */
public class ZipCheckProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ZipCheckProcessor.class);
    /** genome source being tested */
    private GenomeSource genomes;

    // COMMAND-LINE OPTIONS

    /** type of input source */
    @Option(name = "--type", aliases = { "-t", "--source" }, usage = "genome input source type")
    private GenomeSource.Type sourceType;

    /** input genome source */
    @Argument(index = 0, metaVar = "inDir", usage = "file or directory containing the genome source to check",
            required = true)
    private File inDir;

    @Override
    protected void setReporterDefaults() {
        this.sourceType = GenomeSource.Type.MASTER;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        if (! this.inDir.exists())
            throw new FileNotFoundException("Could not find genome source " + this.inDir + ".");
        log.info("Connecting to genome source {}.", this.inDir);
        this.genomes = this.sourceType.create(this.inDir);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Write the report header.
        writer.println("genome_id\terror");
        // Get the list of genome IDs.
        var genomeIds = this.genomes.getIDs();
        // Set up our tracking process.
        int count = 0;
        int processed = 0;
        long lastMessage = System.currentTimeMillis();
        // Test loading each one.
        for (String genomeId : genomeIds) {
            try {
                processed++;
                this.genomes.getGenome(genomeId);
            } catch (Exception e) {
                String errorString = e.toString();
                log.info("Error in genome {}: {}", genomeId, errorString);
                // Here the load failed, so we want to report the genome.
                writer.println(genomeId + "\t" + errorString);
                count++;
            }
            if (System.currentTimeMillis() - lastMessage >= 5000) {
                log.info("{} genomes processed. {} errors.", processed, count);
                lastMessage = System.currentTimeMillis();
            }
        }
        log.info("{} errors found in {} genomes.", count, processed);
    }

}
