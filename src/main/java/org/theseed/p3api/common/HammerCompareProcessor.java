/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.counters.WeightMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This program joins the results of a hammer sample report with the output of a genome.distance run
 * to get an idea of how the hammer counts compare to the distances between the samples and the
 * representative genomes.
 *
 * The hypothesis is that the representative genomes act as beacons.  The further away you are from
 * the representative, the weaker the signal you get from the beacon and the fewer the hammer hits.
 *
 * The positional parameters are the name of the sample report and the name of the distance output
 * file.  For every comparison in both files, the count and the distance will be output.  In the
 * genome.distance run, the samples should be the base genomes and the repgens the compare genomes.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	name of the output file (if not STDOUT)
 *
 */
public class HammerCompareProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerCompareProcessor.class);
    /** map of repgen IDs to names */
    private Map<String, String> nameMap;
    /** sample ID -> rep ID -> hit score */
    private Map<String, WeightMap> hitMap;

    // COMMAND-LINE OPTIIONS

    /** input hammer sample report */
    @Argument(index = 0, metaVar = "sampReport.tbl", usage = "output from hammer sample report", required = true)
    private File sampReportFile;

    /** input distance report */
    @Argument(index = 1, metaVar = "distances.tbl", usage = "output from distance report", required = true)
    private File distReportFile;


    @Override
    protected void setReporterDefaults() {
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Insure we can read both files.
        if (! this.sampReportFile.canRead())
            throw new FileNotFoundException("Input hammer report " + this.sampReportFile + " is not found or unreadable.");
        if (! this.distReportFile.canRead())
            throw new FileNotFoundException("Input distance report " + this.distReportFile + " is not found or unreadable.");
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Load the counts and the repgen names from the hammer report file.
        this.nameMap = new HashMap<String,String>(4000);
        this.hitMap = new HashMap<String, WeightMap>();
        log.info("Reading hit counts from {}.", this.sampReportFile);
        try (TabbedLineReader sampStream = new TabbedLineReader(this.sampReportFile)) {
            int sampIdx = sampStream.findField("sample_id");
            int repIdx = sampStream.findField("repgen_id");
            int repNameIdx = sampStream.findField("repgen_name");
            int hitsIdx = sampStream.findField("count");
            int inCount = 0;
            for (var line : sampStream) {
                inCount++;
                String repId = line.get(repIdx);
                String name = line.get(repNameIdx);
                this.nameMap.put(repId, name);
                String sampId = line.get(sampIdx);
                // Note that the hit count is output as a floating-point score.
                double count = line.getDouble(hitsIdx);
                WeightMap sampleCounts = this.hitMap.computeIfAbsent(sampId, x -> new WeightMap());
                sampleCounts.count(repId, count);
                if (log.isInfoEnabled() && inCount % 5000 == 0)
                    log.info("{} counts processed.", inCount);
            }
            log.info("{} counts processed.  {} samples and {} repgens found.", inCount,
                    this.hitMap.size(), this.nameMap.size());
        }
        // Write the output header.
        writer.println("sample_id\trep_id\trep_name\thammer_score\tdistance");
        log.info("Reading distances from {}.", this.distReportFile);
        try (TabbedLineReader distStream = new TabbedLineReader(this.distReportFile)) {
            int sampIdx = distStream.findField("base_id");
            int repIdx = distStream.findField("genome_id");
            int distIdx = distStream.findField("distance");
            int inCount = 0;
            int skipCount = 0;
            int outCount = 0;
            for (var line : distStream) {
                inCount++;
                String sampId = line.get(sampIdx);
                WeightMap sampWeights = this.hitMap.get(sampId);
                if (sampWeights == null)
                    skipCount++;
                else {
                    String repId = line.get(repIdx);
                    double count = sampWeights.getCount(repId);
                    if (count <= 0.0)
                        skipCount++;
                    else {
                        String name = this.nameMap.getOrDefault(repId, "<unknown>");
                        writer.println(sampId + "\t" + repId + "\t" + name + "\t" + Double.toString(count)
                                + "\t" + line.get(distIdx));
                        outCount++;
                    }
                }
                if (log.isInfoEnabled() && inCount % 10000 == 0)
                    log.info("{} lines read, {} skipped.", inCount, skipCount);
            }
            log.info("{} lines read. {} skipped, {} output.", inCount, skipCount, outCount);
        }
    }

}
