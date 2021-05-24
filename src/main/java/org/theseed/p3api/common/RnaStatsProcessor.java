/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command produces a statistical analysis of the SSR rRNA lengths in a genome directory.  The intent is to
 * apply it to the PATRIC master directory to get an idea of how many have anomalous lengths.
 *
 * The positional parameter is the name of the input genome source directory (or file).
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for report (if not STDOUT)
 * -t	type of genome source (PATRIC, CORE, MASTER, DIR)
 *
 * --min		minimum length for length limit
 * --filter		if specified, a tab-delimited file of genome IDs; only the genome IDs listed in the first column
 * 				will be processed, and they will be processed in the order presented
 *
 * @author Bruce Parrello
 *
 */
public class RnaStatsProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaStatsProcessor.class);
    /** statistics for the RNA lengths */
    private DescriptiveStatistics lengths;
    /** count of lengths below the limit */
    private int shortCount;
    /** input genome source */
    private GenomeSource genomes;
    /** list of genomeIDs to process */
    private List<String> idList;

    // COMMAND-LINE OPTIONS

    /** filter file */
    @Option(name = "--filter", metaVar = "idFile.tbl", usage = "if specified, a tab-delimited file of genome IDs to select")
    private File filterFile;

    /** input genome source type */
    @Option(name = "--type", aliases = { "-t" }, usage = "type of input genome source")
    private GenomeSource.Type sourceType;

    /** minimum RNA length for short-length check */
    @Option(name = "--min", usage = "minimum acceptable RNA length")
    private int minLen;

    /** input genome source */
    @Argument(index = 0, metaVar = "genomeDir", usage = "input genome source (directory or ID file)", required = true)
    private File genomeDir;

    @Override
    protected void setReporterDefaults() {
        this.minLen = 1400;
        this.filterFile = null;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        if (this.minLen < 0)
            throw new ParseFailureException("Minimum length cannot be negative.");
        // Connect to the genome source.
        log.info("Loading genomes at {}.", this.genomeDir);
        this.genomes = this.sourceType.create(this.genomeDir);
        log.info("{} genomes found in {}.", this.genomes.size(), this.genomeDir);
        // If there is a filter file, create the filter set.
        if (this.filterFile == null) {
            log.info("All input genomes will be processed.");
            this.idList = new ArrayList<String>(this.genomes.getIDs());
        } else {
            this.idList = TabbedLineReader.readColumn(this.filterFile, "1");
            log.info("{} genomes specified in filter file {}.", this.idList.size(), this.filterFile);
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Initialize the statistical objects.
        this.shortCount = 0;
        this.lengths = new DescriptiveStatistics();
        // Loop through the genomes.
        int gCount = 0;
        for (String genomeId : this.idList) {
            Genome genome = this.genomes.getGenome(genomeId);
            int len = genome.getSsuRRna().length();
            this.lengths.addValue((double) len);
            if (len < this.minLen)
                this.shortCount++;
            gCount++;
            if (gCount % 100 == 0)
                log.info("{} of {} genomes processed.  {} were too short.", gCount, this.idList.size(), this.shortCount);
        }
        writer.format("%s genomes processed.  %d RNAs were too short (length < %d).%n", gCount, this.shortCount, this.minLen);
        writer.format("Minimum length is %d, maximum is %d, mean is %6.2f, median is %6.2f.%n", (int) this.lengths.getMin(),
                (int) this.lengths.getMax(), this.lengths.getMean(), this.lengths.getPercentile(50.0));
        writer.println();
        writer.println("Percentile\tlength");
        for (double qi = 10.0; qi < 100.0; qi += 10.0)
            writer.format("%6.0f\t%8.2f%n", qi, this.lengths.getPercentile(qi));

        for (double qi = 10.0; qi < 100.0; qi += 10.0) {

        }
    }

}
