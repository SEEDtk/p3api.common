/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleId;
import org.theseed.utils.BasePipeProcessor;

/**
 * This command sets up to fix RNA Seq sample names for the threonine project.  It takes as input a file containing
 * the sample names in the first column and a strain re-assignment file.  It will output a new version of the file
 * with the strain names corrected.
 *
 * The strain reassignments are always a change in host.  Each reassignment entry has an old-name chromosome definition,
 * the old host, and the new host.  "926" is "M" and "277" is "7".  "nrrl XXXXX" hosts are converted to simply "XXXXX".
 * The basic strategy is to convert each reassignment chromosome to a sample ID and map the old chromosome string to
 * the new host.  If an incoming file name's sample ID matches at the chromosome level, we copy the file, renaming it if
 * the host has changed.
 *
 * The positional parameter is the name of the renaming file.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file (if not STDIN)
 * -o	output file (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class RnaFixProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaFixProcessor.class);

    // COMMAND-LINE OPTIONS

    /** name of the renaming definition file */
    @Argument(index = 0, metaVar = "new_strains.txt", usage = "renaming definition file", required = true)
    private File translationFile;

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
        // First, read in the translations.  The map translates a chromosome string to a new host name.  A host name
        // of "?" means the chromosome's samples should be discarded.
        log.info("Loading translation map from {}.", this.translationFile);
        var translationMap = new HashMap<String, String>(500);
        try (var tranStream = new TabbedLineReader(this.translationFile)) {
            for (var line : tranStream) {
                // We need to convert hyphens to "x" in the chromosome definition because of an old amibiguity problem.
                String chromeDef = line.get(0).replace('-', 'x');
                // Create a sample ID from the chromosome definition.
                SampleId sample = SampleId.translate(chromeDef, 24.0, true, "M1").normalizeSets();
                String chromosome = sample.toChromosome();
                // Translate the target host.
                var target = line.get(2);
                if (target.startsWith("nrrl "))
                    target = target.substring(5);
                else if (target.contentEquals("277"))
                    target = "7";
                else if (target.startsWith("?"))
                    target = "?";
                else
                    target = "M";
                // Store the mapping.
                translationMap.put(chromosome, target);
            }
        }
        log.info("{} mappings found.", translationMap.size());
        // Next, we process the input file and produce the output file.
        int count = 0;
        int kept = 0;
        log.info("Processing input lines.");
        for (var line : inputStream) {
            String[] fields = line.getFields();
            count++;
            // Determine the new value for the sample ID.
            SampleId sample = new SampleId(fields[0]).normalizeSets();
            String chrome = sample.toChromosome();
            String target = translationMap.getOrDefault(chrome, "?");
            if (! target.contentEquals("?")) {
                kept++;
                fields[0] = sample.replaceFragment(0, target);
                writer.println(StringUtils.join(fields, '\t'));
            }
        }
        log.info("{} lines read, {} output.", count, kept);
    }

}
