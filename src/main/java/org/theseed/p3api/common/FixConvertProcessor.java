/**
 *
 */
package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleId;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a quick and dirty command that converts the master RNA Seq file produced by "RnaFixProcessor" to
 * an input file for the RNA database metadata processor.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more detailed log messages
 * -i	input file produced by RnaFixProcessor (if not STDIN)
 * -o	output file for metadata input file (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class FixConvertProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FixConvertProcessor.class);


    @Override
    protected void setPipeDefaults() {
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("sample_id\tthr_g/L\tdensity");
        // Loop through the input.
        for (var line : inputStream) {
            String strain = line.get(1);
            // Skip blank strains.  These are bad samples.
            if (! strain.isEmpty()) {
                // Get the IPTG flag.
                boolean iptg = line.getFlag(2);
                // Get the metadata.
                String thr = line.get(4);
                String dens = line.get(5);
                // Fix the sample ID.
                SampleId sample = new SampleId(line.get(0));
                String newHost = StringUtils.substringBefore(strain, "_");
                String realSample = sample.replaceFragment(0, newHost);
                if (! sample.isIPTG() && iptg) {
                    // Here we need to fix the IPTG flag.
                    SampleId temp = new SampleId(realSample);
                    realSample = temp.replaceFragment(SampleId.INDUCE_COL, "I");
                }
                // Write the output line.
                writer.println(realSample + "\t" + thr + "\t" + dens);
            }
        }
    }

}
