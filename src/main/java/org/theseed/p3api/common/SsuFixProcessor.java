/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeMultiDirectory;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Genome;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a simple utility script that fixes bad SSU rRNAs in the PATRIC master genome directory.  Each genome is
 * loaded and the SSU checked to determine if it's badly-formed.  If it is, we re-download the genome to get the
 * correct value and replace the genome in the directory.
 *
 * The positional parameter is the name of the master genome directory.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * @author Bruce Parrello
 *
 */
public class SsuFixProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SsuFixProcessor.class);
    /** master genome directory */
    private GenomeMultiDirectory genomes;
    /** PATRIC connection */
    private P3Connection p3;

    // COMMAND-LINE OPTIONS

    /** name of the PATRIC master directory */
    @Argument(index = 0, metaVar = "inDir", usage = "name of the PATRIC master directory")
    private File inDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Verify the master directory exists.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input master directory " + this.inDir + " is not found or invalid.");
        // Connect to the directory.
        this.genomes = new GenomeMultiDirectory(this.inDir);
        // Connect to PATRIC.
        log.info("Connecting to PATRIC.");
        this.p3 = new P3Connection();
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // This will track our progress.
        long start = System.currentTimeMillis();
        int total = this.genomes.size();
        int count = 0;
        int updated = 0;
        int lastMessage = 0;
        // Now loop through the master directory.
        log.info("Looping through {} genomes.", total);
        for (Genome genome : this.genomes) {
            count++;
            String ssu = genome.getSsuRRna();
            if (! ssu.isEmpty() && ! Genome.isValidSsuRRna(ssu)) {
                // Here we have an invalid SSU rRNA.  Reload and replace the genome.
                log.info("Reloading {}.", genome);
                P3Genome newGenome = P3Genome.load(p3, genome.getId(), P3Genome.Details.STRUCTURE_ONLY);
                genomes.add(newGenome);
                updated++;
                if (log.isInfoEnabled()) {
                    if (count - lastMessage > 100) {
                        Duration d = Duration.ofMillis((System.currentTimeMillis() - start) / count);
                        log.info("{} of {} genomes processed. {} updated. {} per genome.", count, total, updated, d.toString());
                        lastMessage = count;
                    }
                }
            }
            if (count % 500 == 0)
                log.info("{} of {} genomes processed.", count, total);
        }
        log.info("{} genomes updated.", updated);
    }

}
