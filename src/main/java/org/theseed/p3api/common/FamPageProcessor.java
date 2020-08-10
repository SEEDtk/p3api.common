/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeDirectory;
import org.theseed.reports.NaturalSort;
import org.theseed.utils.BaseProcessor;

import j2html.tags.DomContent;

import static j2html.TagCreator.*;
/**
 * This command generates a web page describing the coupled features in a single protein family for a specified
 * input genome directory.
 *
 * The positional parameters are the name of the target protein family and the name of the genome directory.  The
 * web page will be produced on the standard output.
 *
 * The command-line parameters are as follows:
 *
 * -h	display command-line usage
 * -v	show more detailed progress log
 *
 * @author Bruce Parrello
 *
 */
public class FamPageProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FamPageProcessor.class);

    // COMMAND-LINE OPTIONS

    @Argument(index = 0, metaVar = "PGF_0824648", usage = "protein family ID", required = true)
    private String familyId;

    @Argument(index = 1, metaVar = "genomeDir", usage = "input genome directory", required = true)
    private File inDir;

    @Override
    protected void setDefaults() { }

    @Override
    protected boolean validateParms() throws IOException {
        // Verify the input directory.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " not found or invalid.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Connect to the genome directory.
        log.info("Scanning input directory {}.", this.inDir);
        GenomeDirectory genomes = new GenomeDirectory(this.inDir);
        log.info("{} genomes found in directory.", genomes.size());
        // We will stash the features found in here.  Each feature maps to its genome's name.
        Map<String, String> fidSet = new TreeMap<String, String>(new NaturalSort());
        // Loop through the genomes.
        for (Genome genome : genomes) {
            // Loop through the genome's features.
            log.info("Scanning {}.", genome);
            for (Feature feat : genome.getFeatures()) {
                String pgFam = feat.getPgfam();
                if (pgFam != null && this.familyId.contentEquals(pgFam)) {
                    // Here we belong to the correct family.  Check for couplings.
                    if (feat.getCouplings().length > 0)
                        fidSet.put(feat.getId(), genome.toString());
                }
            }
        }
        log.info("{} coupled features found in family {}.", fidSet.size(), this.familyId);
        // Now we start the web page.
        System.out.println("<!DOCTYPE html>");
        System.out.println("<html>");
        String title = "Coupled Features in " + this.familyId;
        System.out.println(header(title(title),
                link().withRel("stylesheet").withHref("css/Basic.css").withType("text/css")).render());
        System.out.println("<body>");
        System.out.println(div().withClass("heading").with(h1(title)).render());
        System.out.println("<div id=\"Pod\">");
        // Create the table.
        List<DomContent> rows = new ArrayList<DomContent>(fidSet.size() + 1);
        rows.add(tr(th("Feature"), th("Genome")));
        for (Map.Entry<String, String> fidEntry : fidSet.entrySet()) {
            String fid = fidEntry.getKey();
            String genome = Feature.genomeOf(fid);
            DomContent fidLink = a(fid).withHref(String.format("coupling.cgi?genome=%s;peg=%s", genome, fid));
            rows.add(tr(td(fidLink), td(text(fidEntry.getValue().toString()))));
        }
        System.out.println(table().with(rows).renderFormatted());
        // Finish the web page.
        System.out.println("</div></body></html>");
    }

}
