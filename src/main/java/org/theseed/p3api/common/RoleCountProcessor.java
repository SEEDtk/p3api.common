/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.counters.CountMap;
import org.theseed.genome.Feature;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.proteins.Role;
import org.theseed.proteins.RoleMap;
import org.theseed.utils.BaseProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command produces a count of the number of times each input role occurs singly in a genome from an input list of genomes.
 * The list of genomes is expected to be large, so we query from the role direction.
 *
 * The positional parameters are the name of a file containing genome IDs in its first column and the name of a role file.
 * (A role file is tab-delimited, with role IDs in the first column and role names in the third.)
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -c	index (1-based) or name of the column in the genome file containing the genome IDs; the default is "genome_id"
 * -s	name of a checkpoint file, containing the ID and count for roles already processed; will be updated with new counts
 *
 * @author Bruce Parrello
 *
 */
public class RoleCountProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RoleCountProcessor.class);
    /** role table */
    private RoleMap roles;
    /** role counts */
    private CountMap<Role> roleCounts;
    /** list of acceptable genome IDs */
    private Set<String> genomes;
    /** connection to PATRIC */
    private P3Connection p3;
    /** counts read from checkpoint file */
    private CountMap<String> oldCounts;
    /** output stream for checkpoint file */
    private PrintWriter checkStream;

    // COMMAND-LINE OPTIONS

    /** identifier for genome ID input column */
    @Option(name = "-c", aliases = { "--col" }, metaVar = "1", usage = "index (1-based) or name of genome file column containing IDs")
    private String genomeCol;

    /** checkpoint file for resuming after errors */
    @Option(name = "-s", aliases = { "--save", "--checkpoint" }, metaVar = "oldCounts.tbl", usage = "name of checkpoint file for save and resume")
    private File checkFile;

    /** name of genome ID file */
    @Argument(index = 0, metaVar = "genomes.tbl", usage = "file of genomes to use for filtering", required = true)
    private File genomeFile;

    /** name of role file */
    @Argument(index = 1, metaVar = "roles.tbl", usage = "file of roles to count", required = true)
    private File roleFile;

    @Override
    protected void setDefaults() {
        this.genomeCol = "genome_id";
        this.checkFile = null;
        this.checkStream = null;
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Read in the genomes.
        if (! genomeFile.canRead())
            throw new FileNotFoundException("Genome file " + this.genomeFile + " not found or unreadable.");
        else try (TabbedLineReader genomeStream = new TabbedLineReader(genomeFile)) {
            log.info("Reading genome IDs from {}.", genomeFile);
            int gCol = genomeStream.findField(this.genomeCol);
            this.genomes = new HashSet<String>(5000);
            for (TabbedLineReader.Line line : genomeStream)
                this.genomes.add(line.get(gCol));
            log.info("{} genome IDs read.", this.genomes.size());
        }
        // Load the roles.
        if (! roleFile.canRead())
            throw new FileNotFoundException("Role file " + this.roleFile + " not found or unreadable.");
        else
            this.roles = RoleMap.load(this.roleFile);
        log.info("{} roles loaded from {}.", this.roles.size(), this.roleFile);
        // Create the role counts.
        this.roleCounts = new CountMap<Role>();
        // Process the checkpoint file.
        this.oldCounts = new CountMap<String>();
        if (this.checkFile != null) {
            if (this.checkFile.canRead()) {
                // Here the checkpoint file already exists.  Read it in and then prepare to append.
                log.info("Restoring old counts from {}.", this.checkFile);
                try (TabbedLineReader checkIn = new TabbedLineReader(this.checkFile, 2)) {
                     for (TabbedLineReader.Line line : checkIn) {
                         String roleId = line.get(0);
                         int count = line.getInt(1);
                         this.oldCounts.setCount(roleId, count);
                     }
                     log.info("{} counts restored from checkpoint.", this.oldCounts.size());
                }
                // Open the checkpoint file to append.
                FileOutputStream checkOut = new FileOutputStream(this.checkFile, true);
                this.checkStream = new PrintWriter(checkOut);
            } else if (this.checkFile.exists())
                throw new IOException("Checkpoint file " + this.checkFile + " exists but is not readable.");
            else {
                // Here there is no checkpoint file, so we open it for output.
                log.info("Checkpointing to {}.", this.checkFile);
                this.checkStream = new PrintWriter(this.checkFile);
            }
        }
        // Connect to PATRIC.
        this.p3 = new P3Connection();
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Loop through the roles.
        for (Role role : this.roles.objectValues()) {
            log.info("Processing role {}.", role);
            // Do we have an old count for it?
            int roleCount = this.oldCounts.getCount(role.getId());
            if (roleCount > 0) {
                // Yes.  Save it.
                this.roleCounts.setCount(role, roleCount);
            } else {
                // No.  Ask the database.  Request all the features with the given role.
                List<JsonObject> features = p3.getRecords(Table.FEATURE, "product", Collections.singleton(role.getName()), "genome_id,patric_id,product");
                //Now we count the number of times the role occurs in each genome.
                CountMap<String> gCounts = new CountMap<String>();
                for (JsonObject feature : features) {
                    // Verify the genome.
                    String genomeId = P3Connection.getString(feature, "genome_id");
                    if (this.genomes.contains(genomeId)) {
                        // Verify the role.
                        String product = P3Connection.getString(feature, "product");
                        List<Role> roles = Feature.usefulRoles(this.roles, product);
                        if (roles.contains(role))
                            gCounts.count(genomeId);
                    }
                }
                // Count the number of times the role occurs singly.
                for (CountMap<String>.Count count : gCounts.counts())
                    if (count.getCount() == 1) this.roleCounts.count(role);
                roleCount = this.roleCounts.getCount(role);
                // Checkpoint the result.
                if (this.checkStream != null) {
                    this.checkStream.format("%s\t%d%n", role.getId(), roleCount);
                    this.checkStream.flush();
                }
            }
            log.info("{} occurrences of role {}.", roleCount, role);
        }
        // Now output the results.
        log.info("Writing results.");
        System.out.println("role_id\trole_name\tcount");
        for (CountMap<Role>.Count count : this.roleCounts.sortedCounts()) {
            Role role = count.getKey();
            System.out.format("%s\t%s\t%8d%n", role.getId(), role.getName(), count.getCount());
        }
    }

}
