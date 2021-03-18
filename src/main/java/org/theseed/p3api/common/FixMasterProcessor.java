/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeMultiDirectory;
import org.theseed.p3api.Connection;
import org.theseed.p3api.Connection.Table;
import org.theseed.p3api.Criterion;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This is a special-purpose command to add SSU rRNA data to old-style master genome directories.
 *
 * The positional parameter is the name of the directory.
 *
 * @author Bruce Parrello
 *
 */
public class FixMasterProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FixMasterProcessor.class);
    /** master genome directory */
    private GenomeMultiDirectory master;

    // COMMAND-LINE OPTIONS

    /** name of the master genome directory */
    @Argument(index = 0, metaVar = "masterDir", usage = "name of the master genome directory")
    private File masterDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Verify the master directory.
        if (! this.masterDir.isDirectory())
            throw new FileNotFoundException("Master directory " + this.masterDir + " is not found or invalid.");
        // Load it for processing.
        this.master = new GenomeMultiDirectory(this.masterDir);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Connect to PATRIC.
        Connection p3 = new Connection();
        int total = this.master.size();
        int processed = 0;
        // Loop through the genomes in the directory.
        for (Genome genome : this.master) {
            processed++;
            if (! genome.checkSsuRRna()) {
                // Here we need to fix this genome.  Get the RNAs.
                log.info("Reading RNAs for {}.", genome);
                List<JsonObject> rnaList = p3.getRecords(Table.FEATURE, "genome_id", Collections.singletonList(genome.getId()),
                        "patric_id,product,na_sequence_md5,na_length", Criterion.EQ("feature_type", "rRNA"));
                int rnaLen = 0;
                String md5 = null;
                // Find the longest SSU rRNA sequence.
                for (JsonObject rna : rnaList) {
                    int naLen = Connection.getInt(rna, "na_length");
                    if (naLen > rnaLen) {
                        String product = Connection.getString(rna, "product");
                        if (Genome.SSU_R_RNA.matcher(product).find()) {
                            rnaLen = naLen;
                            md5 = Connection.getString(rna, "na_sequence_md5");
                        }
                    }
                }
                // If we found a sequence, get it from the database.
                String ssuRRna = "";
                if (rnaLen > 0) {
                    if (md5.isEmpty())
                        log.warn("WARNING:  missing MD5 for RNA in {}.", genome);
                    else {
                        JsonObject sequence = p3.getRecord(Table.SEQUENCE, md5, "sequence");
                        ssuRRna = Connection.getString(sequence, "sequence");
                    }
                }
                genome.setSsuRRna(ssuRRna);
                master.add(genome);
                log.info("{} updated. {} of {} completed.", genome, processed, total);
            }
        }

    }

}
