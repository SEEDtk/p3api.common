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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleId;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command builds a flat-file database for the RNA seq data.  The RNA seq data was originally built with
 * incorrect strain names and threonine output in mg/L.  The database output will show the corrected strain ID
 * and threonine output in g/L.
 *
 * The positional parameter is the name of the strain-mapping file.  This is a tab-delimited file with headers that has
 * an old-style chromosome definition in the first column and a corrected host name in the third column.  A host name of
 * "926" denotes "M", a host name of "277" denotes "7", and a host name of "???" denotes an unknown host.
 *
 * The standard input should contain an incoming RNA seq metadata file.  The resulting report will be on the standard
 * output.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing metadata (if not STDIN)
 * -o 	output file for report (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class ReStrainMapProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ReStrainMapProcessor.class);
    /** mapping of chromosome IDs to corrected hosts */
    private Map<String, String> correctionMap;
    /** input column index for sample ID */
    private int sampleColIdx;
    /** input column index for threonine level */
    private int thrColIdx;
    /** input column index for density */
    private int densColIdx;
    /** map of host numbers to host IDs */
    private static final Map<String, String> HOST_MAP = Map.of("926", "M", "277", "7", "???", "");
    /** set of engineered hosts */
    private static final Set<String> ENGINEERED_HOSTS = Set.of("7", "M");

    // COMMAND-LINE OPTIONS

    /** name of the strain-renaming file */
    @Argument(index = 0, metaVar = "new_strain_mapping.tbl", usage = "strain correction file")
    private File strainFile;

    @Override
    protected void setPipeDefaults() {
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Insure we have all the input columns we need.
        this.sampleColIdx = inputStream.findField("sample");
        this.thrColIdx = inputStream.findField("thr_mg/L");
        this.densColIdx = inputStream.findField("OD_600nm");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the mapping file.
        if (! this.strainFile.canRead())
            throw new FileNotFoundException("Strain-mapping file " + this.strainFile + " is not found or unreadable.");
        // Read it in to create the map.
        this.correctionMap = new HashMap<String, String>(400);
        int lineCount = 0;
        int skipCount = 0;
        try (var mapStream = new TabbedLineReader(this.strainFile)) {
            // Loop through the file.  We skip any line that begins with a pound sign (#).
            for (var line : mapStream) {
                String oldChrome = line.get(this.sampleColIdx);
                if (! oldChrome.startsWith("#")) {
                    lineCount++;
                    // Here we have a non-comment line.  Parse it and extract the chromosome ID.
                    var newChrome = SampleId.translate(oldChrome, 24.0, false, "M1").toChromosome();
                    // Get the target host.
                    var host = HOST_MAP.getOrDefault(line.get(2), "");
                    if (host.isEmpty()) skipCount++;
                    this.correctionMap.put(newChrome, host);
                }
            }
            log.info("{} data lines read from {}. {} skips stored, {} total mappings.",
                    lineCount, this.strainFile, skipCount, this.correctionMap.size());
        }
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("sample_id\tstrain\tIPTG\ttime\tthr_g/L\tOD_600nm");
        // Loop through the input file.
        int lineCount = 0;
        int skipCount = 0;
        int changeCount = 0;
        for (var line : inputStream) {
            lineCount++;
            SampleId sample = new SampleId(line.get(0));
            String oldChrome = sample.toChromosome();
            String oldHost = sample.getFragment(0);
            // Compute the new host.  Only engineered hosts can change.
            String newHost;
            if (ENGINEERED_HOSTS.contains(oldHost))
                newHost = this.correctionMap.getOrDefault(oldChrome, "");
            else
                newHost = oldHost;
            // Compute the corrected strain ID.
            String strain;
            if (newHost.isEmpty()) {
                // Strain is ambiguous.  Use an empty string.
                strain = "";
                skipCount++;
            } else {
                strain = sample.toStrain();
                if (! newHost.equals(sample.getFragment(0))) {
                    // Host has changed.  Replace the first character of the strain ID with
                    // the correct host character.
                    changeCount++;
                    strain = newHost + "_" + StringUtils.substringAfter(strain, "_");
                }
            }
            // Fix the threonine count.
            double threonine = line.getDouble(this.thrColIdx) / 1000.0;
            // Copy the density.  We keep the formatting.
            String density = line.get(this.densColIdx);
            // Get the IPTG and time point.
            double timePoint = sample.getTimePoint();
            String iptg = ((sample.isIPTG() && timePoint >= 5.0) ? "Y" : "");
            // Write the output line.
            writer.format("%s\t%s\t%s\t%4.1f\t%6.4f\t%s%n", sample.toString(), strain, iptg, timePoint,
                    threonine, density);
        }
        log.info("{} lines read, {} bad strains, {} strains changed.", lineCount, skipCount, changeCount);
    }


}
