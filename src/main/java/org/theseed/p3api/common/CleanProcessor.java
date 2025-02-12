/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.GenomeMultiDirectory;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3Connection;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command takes as input a master genome directory and removes the genomes not currently in the list of PATRIC
 * prokaryotes.  The positional parameter is the name of the master genome directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	show more detailed progress messages
 *
 * @author Bruce Parrello
 *
 */
public class CleanProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CleanProcessor.class);

    // COMMAND-LINE OPTIONS

    /** master genome directory **/
    @Argument(index = 0, metaVar = "inDir", usage = "name of the master genome directory to process")
    private File inDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Get the main genome directory.
        GenomeMultiDirectory genomes = new GenomeMultiDirectory(this.inDir);
        log.info("{} genomes found in {}.", genomes.size(), this.inDir);
        // Get a list of the genomes to delete.
        Collection<String> deleteSet = this.getDeleteSet(genomes);
        // Remove them from the directory.
        for (String genomeId : deleteSet) {
            log.info("Removing {}.",genomeId);
            genomes.remove(genomeId);
        }
        log.info("{} genomes remaining in {}.", genomes.size(), this.inDir);
    }

    /**
     * @return the IDs of the genomes to delete
     *
     * @param genomes	the input genomes
     */
    private Set<String> getDeleteSet(GenomeMultiDirectory genomes) {
        List<JsonObject> goodList = new ArrayList<JsonObject>(genomes.size());
        P3Connection p3 = new P3Connection();
        p3.addAllProkaryotes(goodList);
        log.info("{} eligible genomes in PATRIC.", goodList.size());
        // Get the set of genome IDs in the input directory.
        Set<String> retVal = new HashSet<String>(genomes.getIDs());
        // Remove the good ones, leaving only the bad ones.
        for (JsonObject good : goodList) {
            String goodId = KeyBuffer.getString(good, "genome_id");
            retVal.remove(goodId);
        }
        log.info("{} genomes will be deleted.", retVal.size());
        return retVal;
    }

}
