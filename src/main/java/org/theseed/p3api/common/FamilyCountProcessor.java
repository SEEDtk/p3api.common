/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.counters.CountMap;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeDirectory;

/**
 * This command counts the protein families in the genomes of a genome directory.  The output
 * is a table of the family counts in order.  The positional parameter is the input genome directory.
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	show more detailed progress messages
 *
 * @author Bruce Parrello
 *
 */
public class FamilyCountProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FamilyCountProcessor.class);
    /** protein family counter */
    private CountMap<String> familyCounts;
    /** input genome directory */
    private GenomeDirectory genomes;

    // COMMAND-LINE OPTIONS

    /** input directory */
    @Argument(index = 0, metaVar = "gtoDir", usage = "input directory of GTOs")
    private File inDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Connect to the genome directory.
        if (! inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " not found or invalid.");
        this.genomes = new GenomeDirectory(this.inDir);
        log.info("{} genomes found in {}.", this.genomes.size(), this.inDir);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Initialize the count map.
        this.familyCounts = new CountMap<String>();
        // We will track the number of features containing families.
        int genomeCount = 0;
        int pegCount = 0;
        // Process the genomes.
        for (Genome genome : this.genomes) {
            genomeCount++;
            log.info("Scanning genome {}: {}.", genomeCount, genome);
            for (Feature feat : genome.getFeatures()) {
                String family = feat.getPgfam();
                if (family != null && ! family.isEmpty()) {
                    this.familyCounts.count(family);
                    pegCount++;
                }
            }
        }
        // Now we output the counts.
        log.info("{} family-containing features found in {} genomes.", pegCount, genomeCount);
        log.info("{} families found, averaging {} features each.", this.familyCounts.size(),
                ((double) pegCount) / this.familyCounts.size());
        System.out.println("family_id\tcount");
        for (CountMap<String>.Count count : this.familyCounts.sortedCounts())
            System.out.format("%s\t%8d%n", count.getKey(), count.getCount());
    }

}
