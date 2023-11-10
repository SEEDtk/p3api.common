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

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Feature;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * This is a utility command to convert hammer strengths to max(N, 42) / 42, where "N" is the repgen's neighborhood size.
 * This method puts higher weight on hammers relating to genomes for which we know the other hammers are unlikely to conflict.
 * The radix 42 is the mean neighborhood size.
 *
 * The standard input should contain the old hammer load file.  The new file will be output on the standard output.
 *
 * The positional parameter should be the name of the appropriate repXX.stats.tbl file.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input hammer load file (if not STDIN)
 * -o	output hammer load file (if not STDOUT)
 *
 * --size	mean neighborhood size to use (default 42)
 *
 * @author Bruce Parrello
 *
 */
public class HammerFixProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerFixProcessor.class);
    /** map from repgen IDs to strength factors */
    private Map<String, Double> scaleMap;
    /** hammer feature ID column index */
    private int fidColIdx;
    /** hammer strength column index */
    private int strengthColIdx;
    /** denominator for scale factors */
    private double denom;

    // COMMAND-LINE OPTIONS

    /** mean neighborhoos size for scaling */
    @Option(name = "--size", metaVar = "100", usage = "mean neighborhood size to use in scaling")
    private int meanSize;

    /** repgen stats file containing neighborhood sizes */
    @Argument(index = 0, metaVar = "repXX.stats.tbl", usage = "repXX.stats.tbl file containing representative-genome neighborhood sizes")
    private File repStatsFile;

    @Override
    protected void setPipeDefaults() {
        this.meanSize = 42;
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Insure this is a valid hammer load file.
        this.fidColIdx = inputStream.findField("fid");
        this.strengthColIdx = inputStream.findField("strength");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the mean neighborhood size.
        if (this.meanSize < 2)
            throw new ParseFailureException("Mean neighborhood size must be at least 2.");
        // Invert the mean size to get the denominator.
        this.denom = 1.0 / this.meanSize;
        // Validate and load the neighborhood sizes.
        if (! this.repStatsFile.canRead())
            throw new FileNotFoundException("Repgen stats file " + this.repStatsFile + " is not found or unreadable.");
        log.info("Loading neighborhood data from {}.", this.repStatsFile);
        try (TabbedLineReader repStream = new TabbedLineReader(this.repStatsFile)) {
            int repIdColIdx = repStream.findField("rep_id");
            int memColIdx = repStream.findField("members");
            this.scaleMap = new HashMap<String, Double>(5000);
            for (var line : repStream) {
                // Read the current repgen and compute its scale factor.
                String repId = line.get(repIdColIdx);
                int members = line.getInt(memColIdx);
                double scale = Math.min(members, this.meanSize) * this.denom;
                this.scaleMap.put(repId, scale);
            }
        }
        log.info("{} scale factors stored.", this.scaleMap.size());
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println(inputStream.header());
        // Now loop through the hammers, applying the scale factor.
        long timer = System.currentTimeMillis();
        int inCount = 0;
        for (var line : inputStream) {
            // Get the repgen ID for this hammer.
            String fid = line.get(this.fidColIdx);
            String repId = Feature.genomeOf(fid);
            // Compute the strength.  Note we default to the denominator value (neighborhood size of 1).
            String newStrength = Double.toString(this.scaleMap.getOrDefault(repId, this.denom));
            // Output the new line.
            String[] fields = line.getFields();
            fields[this.strengthColIdx] = newStrength;
            writer.println(StringUtils.join(fields, '\t'));
            inCount++;
            if (log.isInfoEnabled() && System.currentTimeMillis() - timer >= 5000) {
                timer = System.currentTimeMillis();
                log.info("{} hammers updated.", inCount);
            }
        }
        log.info("{} hammers updated.", inCount);
    }

}
