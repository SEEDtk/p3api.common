/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeDirectory;
import org.theseed.p3api.Connection;
import org.theseed.p3api.Connection.Table;
import org.theseed.utils.BaseProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command compares the subsystems in a GTO to the corresponding subsystems in PATRIC.  The positional
 * parameter is the name of a genome directory.  All of the genomes in the directory will be checked.
 *
 * The command-line options are
 *
 * -h	show command-line usage
 * -v	display more detailed log messages
 *
 * @author Bruce Parrello
 *
 */
public class SubsystemCheckProcessor extends BaseProcessor {

    // COMMAND-LINE OPTIONS
    @Argument(index = 0, metaVar = "gtoDir", usage = "genome input directory", required = true)
    private File inDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException {
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        int total = 0;
        int missCount = 0;
        // Get the genome directory.
        GenomeDirectory genomes = new GenomeDirectory(this.inDir);
        log.info("{} genomes in input directory.", genomes.size());
        // Connect to PATRIC.
        Connection p3 = new Connection();
        // Write the report header.
        System.out.println("genome\tfeature_id\trole\tpgfam\tmissing_subsystem");
        // Loop through the genomes.
        for (Genome genome : genomes) {
            log.info("Processing {}.", genome);
            List<JsonObject> subsystemItems = p3.getRecords(Table.SUBSYSTEM_ITEM, "genome_id", Collections.singleton(genome.getId()),
                    "patric_id,subsystem_name");
            for (JsonObject record : subsystemItems) {
                String fid = Connection.getString(record, "patric_id");
                String subName = Connection.getString(record, "subsystem_name");
                Feature feat = genome.getFeature(fid);
                if (feat != null) {
                    total++;
                    Set<String> featSubs = feat.getSubsystems();
                    if (! featSubs.contains(subName)) {
                        missCount++;
                        String pgFam = feat.getPgfam();
                        if (pgFam == null) pgFam = "";
                        System.out.format("%s\t%s\t%s\t%s\t%s%n", genome.getId(), fid, feat.getFunction(), pgFam, subName);
                    }
                }
            }
        }
        log.info("All done.  {} features checked, {} misses.", total, missCount);
    }

}
