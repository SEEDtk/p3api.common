/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Connection;
import org.theseed.p3api.Connection.Table;
import org.theseed.p3api.Criterion;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This is a special-purpose command to fix an error in patric.qual.tbl.
 *
 * The positional parameters are the names of the input and output files
 *
 * @author Bruce Parrello
 *
 */
public class FixMasterProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FixMasterProcessor.class);

    // COMMAND-LINE OPTIONS

    /** name of the input file */
    @Argument(index = 0, metaVar = "inFile.tbl", usage = "input quality file")
    private File inFile;

    /** name of the output file */
    @Argument(index = 1, metaVar = "outFile.tbl", usage = "corrected quality file")
    private File outFile;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Verify the input directory.
        if (! this.inFile.canRead())
            throw new FileNotFoundException("Input file " + this.inFile + " is not found or unreadable.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Get the cds counts for all the genomes.
        Connection p3 = new Connection();
        List<JsonObject> genomes = p3.getRecords(Table.GENOME, "kingdom", Connection.DOMAINS,
                "genome_id,patric_cds", Criterion.EQ("public", "1"),
                Criterion.NE("genome_status", "Plasmid"));
        Map<String, Integer> counts = new HashMap<String, Integer>(genomes.size());
        for (JsonObject genome : genomes) {
            String id = Connection.getString(genome, "genome_id");
            int count = Connection.getInt(genome, "patric_cds");
            if (count > 0)
                counts.put(id, count);
            else
                log.warn("Missing count for {}.", id);
        }
        try (TabbedLineReader inStream = new TabbedLineReader(this.inFile);
                PrintWriter writer = new PrintWriter(this.outFile)) {
            writer.println(StringUtils.join(inStream.getLabels(), '\t'));
            int count = 0;
            for (TabbedLineReader.Line line : inStream) {
                String id = line.get(0);
                int hypoCount = line.getInt(8);
                Integer pegs = counts.get(id);
                if (pegs == null)
                    pegs = hypoCount * 2;
                double hypo = (hypoCount * 100.0) / pegs;
                String[] fields = line.getFields();
                fields[8] = String.format("%6.2f", hypo);
                writer.println(StringUtils.join(fields, '\t'));
                count++;
                if (count % 1000 == 0)
                    log.info("{} genomes processed.", count);
            }
        }
    }

}
