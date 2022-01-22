/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a simple command that takes a list of gene names as input and finds
 * the b-number IDs for those genes.  The gene names should come in on the standard
 * input in the first column of a tab-delimited file with headers.  The single
 * positional parameter is the name of a genome GTO file containing the features
 * of interest.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more detailed log messages
 * -i	input file (if not STDIN)
 * -o	output file (if not STDOUT)
 *
 * @author Bruce Parrello
 *
 */
public class BFinderProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BFinderProcessor.class);
    /** genome of interest */
    private Genome genome;
    /** alias map */
    private Map<String, Set<String>> aliasMap;
    /** match pattern for b-numbers */
    private static final Pattern B_NUM = Pattern.compile("b\\d+");

    // COMMAND-LINE OPTIONS

    /** file name for target genome */
    @Argument(index = 0, metaVar = "genome.gto", usage = "input genome GTO file",
            required = true)
    private File genomeFile;

    @Override
    protected void setPipeDefaults() {
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        if (! this.genomeFile.canRead())
            throw new FileNotFoundException("Genome file " + this.genome + " is not found or unreadable.");
        log.info("Loading genome from {}.", this.genomeFile);
        this.genome = new Genome(this.genomeFile);
        log.info("Processing aliases from {}.", this.genome);
        this.aliasMap = new HashMap<String, Set<String>>(this.genome.getFeatureCount());
        for (Feature feat : this.genome.getPegs()) {
            var aliases = feat.getAliases();
            // Look for a b-number in the aliases.
            Set<String> others = new TreeSet<String>();
            String bNum = null;
            for (String alias : aliases) {
                if (B_NUM.matcher(alias).matches())
                    bNum = alias;
                else
                    others.add(alias);
            }
            // Only track features with b-numbers.
            if (bNum != null) {
                // Add this b-number to the feature's aliases.
                for (String alias : others) {
                    Set<String> bNumSet = this.aliasMap.computeIfAbsent(alias, x -> new TreeSet<String>());
                    bNumSet.add(bNum);
                }
            }
        }
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        writer.println("alias\tb-number");
        // This set will prevent duplicate b-numbers.
        Set<String> bNumsUsed = new HashSet<String>(this.genome.getFeatureCount());
        // Loop through the input.
        for (TabbedLineReader.Line line : inputStream) {
            String alias = line.get(0);
            // Get all the b-numbers for this alias.
            var bNums = this.aliasMap.get(alias);
            if (bNums == null)
                log.warn("No b-number found for \"{}\".", alias);
            else {
                for (String bNum : bNums) {
                    if (! bNumsUsed.contains(bNum)) {
                        writer.println(bNum + "\t" + alias);
                        bNumsUsed.add(bNum);
                    }
                }
            }
        }
        log.info("{} b-numbers found.", bNumsUsed.size());
    }

}
