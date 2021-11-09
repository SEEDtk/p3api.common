/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Option;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.P3Connection;
import org.theseed.p3api.P3Connection.Table;
import org.theseed.utils.BaseProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This class processes a request to count the protein families in one or more subsystems.
 * The report will count the distinct protein families for subsystem and the distinct
 * protein families for all subsystems.
 *
 * The standard input (or the input file if one is specified) should be tab-delimited with
 * headers.  The subsystem IDs will be taken from the first column by default.
 *
 * The command-line options are as follows.
 *
 * -i	input file (if not STDIN)
 * -c	the index (1-based) or name of the column containing the subsystem IDs; the default
 * 		is the first column
 * -v	display more detailed log messages
 *
 * @author Bruce Parrello
 *
 */
public class SubFamilyProcessor extends BaseProcessor {

    // FIELDS
    /** set of protein families found in all subsystems */
    private Set<String> allFamilies;
    /** index of input key column */
    private int keyIdx;
    /** input stream */
    TabbedLineReader inStream;
    /** PATRIC connection */
    private P3Connection p3;

    // COMMAND LINE

    /** input file (if not STDIN) */
    @Option(name = "-i", aliases = { "--input" }, metaVar = "inFile", usage = "name of input file (default is to use STDIN)")
    private File inFile;

    /** subsystem ID column */
    @Option(name = "-c", aliases = { "--col" }, metaVar = "subsystem_id", usage = "index (1-based) or name of column with subsystem IDs")
    private String keyCol;

    @Override
    protected void setDefaults() {
        this.keyCol = "1";
        this.inFile = null;

    }

    @Override
    protected boolean validateParms() throws IOException {
        if (this.inFile != null) {
            if (! this.inFile.canRead())
                throw new FileNotFoundException("Input file " + this.inFile + " is not found or unreadable.");
            else {
                log.info("Reading subsystem IDs from {}.", this.inFile);
                this.inStream = new TabbedLineReader(this.inFile);
            }
        } else {
            log.info("Reading subsystem IDs from standard input.");
            this.inStream = new TabbedLineReader(System.in);
        }
        // Find the input column.
        this.keyIdx = this.inStream.findField(this.keyCol);
        return true;
    }

    @Override
    public void runCommand() {
        // Connect to PATRIC.
        this.p3 = new P3Connection();
        // Create the master family set.
        this.allFamilies = new HashSet<String>();
        System.out.println("subsystem_id\tfamilies");
        // Loop through the subsystems.
        for (TabbedLineReader.Line line : this.inStream) {
            String subsystem = line.get(this.keyIdx);
            // Get the set of families for this subsystem.
            Set<String> families = this.countSubsystem(subsystem);
            // Process the counts.
            System.out.format("%s\t%d%n", subsystem, families.size());
            this.allFamilies.addAll(families);
        }
        // Print the total.
        System.out.println();
        System.out.format("TOTAL\t%d%n", this.allFamilies.size());
    }

    private Set<String> countSubsystem(String subsystem) {
        log.info("Counting subsystem {}.", subsystem);
        // Get all the features for the subsystem.
        Collection<JsonObject> features = p3.getRecords(Table.SUBSYSTEM_ITEM, "subsystem_id", Collections.singleton(subsystem), "patric_id");
        // Get the protein family for each feature.
        Collection<String> fids = features.stream().map(x -> P3Connection.getString(x, "patric_id")).collect(Collectors.toSet());
        Map<String, JsonObject> families = p3.getRecords(Table.FEATURE, fids, "pgfam_id");
        // Convert the protein family IDs into a set.
        Set<String> retVal = families.values().stream().map(x -> P3Connection.getString(x, "pgfam_id")).collect(Collectors.toSet());
        return retVal;
    }

}
