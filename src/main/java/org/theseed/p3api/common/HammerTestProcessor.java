/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.StringPair;

/**
 * This command reads the misses file and checks the distances between the genomes.  If the ANI distance between
 * the actual genome and the test genome is less than or equal to the distance between the expected genome and the
 * test genome, the miss is considered a good hit.  The incoming report is augmented with extra columns showing the
 * distances and whether the miss is acceptable or a genuine error.
 *
 * The standard input should contain the "--misses" output from "ContigTestAnalysisProcessor".  The positional
 * parameter is the name of a file containing the computed distances.  The augmented misses file will be written
 * to the standard output.
 *
 * In the distance file, the genome IDs should be in columns named "id1" and "id2".  The distance will be taken
 * from a column identified by a command-line option.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file (if not STDIN)
 * -o	output file (if not STDOUT)
 *
 * --type	type of distance to use (default ANI_chunk1020.I30,M>35)
 *
 * @author Bruce Parrello
 *
 */
public class HammerTestProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerTestProcessor.class);
    /** map of genome ID pairs to distances */
    private Map<StringPair, Double> distanceMap;
    /** index of the test genome ID column */
    private int testGenomeIdx;
    /** index of the found genome ID column */
    private int foundGenomeIdx;
    /** index of the expected genome ID column */
    private int repGenomeIdx;

    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of column containing  distance to use */
    @Option(name = "--type", metaVar = "Genes_K20:PhenTrnaSyntAlph", usage = "index (1-based) or name of distance input column")
    private String distanceCol;

    /** name of the distances file */
    @Argument(index = 0, metaVar = "distances.tbl", usage = "name of the file containing the distances")
    private File distFile;

    @Override
    protected void setPipeDefaults() {
        this.distanceCol = "ANI_chunk1020.I30,M>35";
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Get the useful columns.
        this.foundGenomeIdx = inputStream.findField("actual");
        this.repGenomeIdx = inputStream.findField("expected");
        this.testGenomeIdx = inputStream.findField("genome_id");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Verify the distance file and load the distances.
        if (! this.distFile.canRead())
            throw new FileNotFoundException("Distance file " + this.distFile + " is not found or unreadable.");
        try (TabbedLineReader distStream = new TabbedLineReader(this.distFile)) {
            log.info("Reading distances from {}.", this.distFile);
            // Locate the genome ID columns.
            int id1ColIdx = distStream.findField("id1");
            int id2ColIdx = distStream.findField("id2");
            // Locate the distance column.
            int distColIdx = distStream.findField(this.distanceCol);
            // Create the distance map.
            this.distanceMap = new HashMap<StringPair, Double>(2000);
            // Fill it from the distance file.
            for (var line : distStream) {
                StringPair genomes = new StringPair(line.get(id1ColIdx), line.get(id2ColIdx));
                double distance = line.getDouble(distColIdx);
                this.distanceMap.put(genomes, distance);
            }
            log.info("{} distances loaded from {}.", this.distanceMap.size(), this.distFile);
        }
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println(inputStream.header() + "\ttest_to_actual\ttest_to_rep\tgood_hit");
        // Loop through the input file, comparing distances.
        int goodCount = 0;
        int badCount = 0;
        for (var line : inputStream) {
            // Compute the test-to-actual distance.
            String testGenome = line.get(testGenomeIdx);
            String actualGenome = line.get(foundGenomeIdx);
            double testToActual = this.getDistance(testGenome, actualGenome);
            // Compute the test-to-expected distance.
            String expectedGenome = line.get(repGenomeIdx);
            double testToExpected = this.getDistance(testGenome, expectedGenome);
            // Compare the distances.
            String goodFlag;
            if (testToActual <= testToExpected) {
                goodFlag = "Y";
                goodCount++;
            } else {
                goodFlag = "";
                badCount++;
            }
            writer.println(line.getAll() + String.format("\t%6.4f\t%6.4f\t%s", testToActual, testToExpected, goodFlag));
        }
        log.info("{} good misses, {} bad misses.", goodCount, badCount);
    }

    /**
     * Get the distance between the two genomes.
     *
     * @param g1	ID of the first genome
     * @param g2	ID of the second genome
     *
     * @return the distance from the distance file, or 2.0 if the distance is missing
     */
    private double getDistance(String g1, String g2) {
        StringPair pair = new StringPair(g1, g2);
        double retVal = this.distanceMap.getOrDefault(pair, 2.0);
        if (retVal == 2.0)
            log.warn("Missing distance between {} and {}.", g1, g2);
        return retVal;
    }

}
