/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseProcessor;
import org.theseed.sequence.ProteinKmers;

/**
 * This command creates a chart of bin quality against PheS distance.  For each bin, it displays the PheS distance between the
 * bin and the reference genome, then shows the three bin quality numbers and a combined score.
 *
 * The positional parameter is the name of the master binning directory.  All of its subdirectories will be processed.
 *
 * The command-line options are as follows:
 *
 * -h	display usage
 * -v	display more detailed progress messages on the log
 *
 * @author Bruce Parrello
 *
 */
public class BinCheckProcessor extends BaseProcessor {

    // FIELDS

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BinCheckProcessor.class);

    /** map of genome IDs to PheS objects */
    private Map<String, ProteinKmers> seedMap;

    // COMMAND-LINE OPTIONS
    @Argument(index = 0, metaVar = "masterDir", usage = "master directory of binning sub-directories")
    private File masterDir;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException {
        // Verify that the binning directory is valid.
        if (! this.masterDir.isDirectory())
            throw new FileNotFoundException(this.masterDir + " was not found or is not a valid directory.");
        return true;
    }

    // This is a file-name filter that selects the genome files in a binning directory.
    private static class GtoFilter implements FilenameFilter {

        public boolean accept(File dir, String name) {
            return (name.endsWith(".gto") || name.matches("\\d+\\.\\d+\\.json"));
        }
    }

    @Override
    protected void runCommand() throws Exception {
        // Write the output header.
        System.out.println("sample\tbin_id\tbin_name\tdistance\tconsistency\tcompleteness\tcontamination\tgood");
        // Create the filename filter.
        FilenameFilter genomeFilter = new GtoFilter();
        // Create the main hash map.
        this.seedMap = new HashMap<String, ProteinKmers>(50);
        // Get all the subdirectories of the master.
        File[] binDirs = this.masterDir.listFiles(File::isDirectory);
        log.info("{} binning subdirectories found in {}.", binDirs.length, this.masterDir);
        // Loop through them.
        for (File binDir : binDirs) {
            // Verify that we have an index file.
            File indexFile = new File(binDir, "Eval/index.tbl");
            if (! indexFile.canRead()) {
                log.warn("No index file found for {}.", binDir);
            } else {
                // Read in the bins.  For each bin, we remember its PheS.
                this.seedMap.clear();
                for (File genomeFile : binDir.listFiles(genomeFilter)) {
                    Genome genome = new Genome(genomeFile);
                    log.info("Searching for seed protein in {}.", genome);
                    String seedProt = this.getSeedProtein(genome);
                    if (seedProt == null) {
                        log.warn("No seed protein found in {}.", genome);
                    } else {
                        ProteinKmers kmers = new ProteinKmers(seedProt);
                        this.seedMap.put(genome.getId(), kmers);
                    }
                }
                // Read in the index file.
                try (TabbedLineReader indexStream = new TabbedLineReader(indexFile)) {
                    String sampName = binDir.getName();
                    log.info("Processing bin {}.", sampName);
                    for (TabbedLineReader.Line line : indexStream) {
                        // Get the genome information.
                        String binId = line.get(1);
                        String binName = line.get(2);
                        String refId = line.get(3);
                        // Get the quality metrics.
                        double consistency = line.getDouble(9);
                        double completeness = line.getDouble(10);
                        double contamination = line.getDouble(11);
                        boolean goodFlag = line.getFlag(14);
                        // Get the proteins.
                        ProteinKmers binSeed = this.seedMap.get(binId);
                        ProteinKmers refSeed = this.seedMap.get(refId);
                        double distance = 1.0;
                        if (binSeed != null && refSeed != null)
                            distance = binSeed.distance(refSeed);
                        // Write the output.
                        System.out.format("%s\t%s\t%s\t%4.3f\t%6.2f\t%6.2f\t%6.2f\t%b%n",
                                sampName, binId, binName, distance, consistency, completeness, contamination, goodFlag);
                    }
                }
            }
        }
    }

    /**
     * @return the seed protein for the specified genome, or NULL if none was found
     *
     * @param genome	genome to search for a seed protein
     */
    private String getSeedProtein(Genome genome) {
        String retVal = "";
        for (Feature peg : genome.getPegs()) {
            if (peg.getFunction().contentEquals("Phenylalanyl-tRNA synthetase alpha chain (EC 6.1.1.20)")) {
                String newProt = peg.getProteinTranslation();
                if (newProt.length() >= retVal.length())
                    retVal = newProt;
            }
        }
        if (retVal.isEmpty()) retVal = null;
        return retVal;
    }

}
