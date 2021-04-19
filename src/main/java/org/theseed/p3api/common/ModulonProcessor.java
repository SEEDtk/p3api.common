/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.SubsystemRow;
import org.theseed.io.LineReader;
import org.theseed.io.TabbedLineReader;
import org.theseed.magic.MagicMap;
import org.theseed.subsystems.SubsystemRowDescriptor;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a one-off utility script to hook up gene IDs in the MG1655 wild type with atomic regulon numbers, subsystems, and
 * iModulon names.  The atomic regulon numbers are associated with FIG IDs in the CoreSEED 83333.1 genome.  These
 * have to be translated to b-numbers so they can be associated with the wild type genes.
 *
 * The basic strategy is get the features from the CoreSEED genome and map them to b-numbers.  Then we map the
 * b-numbers to modulon IDs.  Finally, the b-numbers are mapped to the wild-type feature IDs.
 * The final table contains the wild-type FIG ID, iModulon name, and AR number.
 *
 * The positional parameters are the name of the wild type GTO, the name of the CoreSEED GTO, and the
 * names of the atomic regulon and imodulon files.
 *
 * The atomic regulon file is tab-delimited with headers.  The first column is the AR number and the second is the
 * CoreSEED FIG ID.
 *
 * The iModulon file is comma-separated.  The first column is the modulon name and the remaining columns are the
 * b-numbers of the modulon's genes.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file stream
 *
 * @author Bruce Parrello
 *
 */
public class ModulonProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ModulonProcessor.class);
    /** map of CoreSEED fig IDS to b-numbers */
    private Map<String, String> coreBMap;
    /** map of b-numbers to iModulons */
    private Map<String, List<String>> modulonMap;
    /** map of b-numbers to wild-type fig IDs */
    private Map<String, String> bNumFigMap;
    /** map of b-numbers to subsystems */
    private Map<String, String> bNumSubMap;
    /** CoreSEED genome */
    private Genome coreGenome;
    /** output file stream */
    private OutputStream outStream;
    /** number of features in an E coli genome */
    private static final int NUM_FEATURES = 4200;
    /** b-number pattern matcher */
    private static final Pattern B_NUMBER = Pattern.compile("b\\d+");

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT */
    @Option(name = "--output", aliases = { "-o" }, usage = "output file name (if not STDOUT)")
    private File outFile;

    /** name of the GTO for the RNA seq reference E coli */
    @Argument(index = 0, metaVar = "wildType.gto", usage = "GTO for the main reference E coli genome")
    private File wildFile;

    /** name of the GTO for the atomic regulon reference E coli */
    @Argument(index = 1, metaVar = "coreSeed.gto", usage = "GTO for the CoreSEED reference E coli genome")
    private File coreFile;

    /** name of the file containing the atomic regulons */
    @Argument(index = 2, metaVar = "arFile.tbl", usage = "atomic regulon table")
    private File arFile;

    /** name of the file containing the iModulons */
    @Argument(index = 3, metaVar = "imodulon.csv", usage = "iModulon table")
    private File modFile;

    @Override
    protected void setDefaults() {
        this.outFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // We must verify that all the input files exist.
        if (! this.wildFile.canRead())
            throw new FileNotFoundException("Wild-type GTO file " + this.wildFile + " is not found or unreadable.");
        if (! this.coreFile.canRead())
            throw new FileNotFoundException("CoreSEED GTO file " + this.coreFile + " is not found or unreadable.");
        if (! this.arFile.canRead())
            throw new FileNotFoundException("Atomic regulon file " + this.arFile + " is not found or unreadable.");
        if (! this.modFile.canRead())
            throw new FileNotFoundException("Modulon file " + this.modFile + " is not found or unreadable.");
        // Create the output file stream.
        if (this.outFile != null) {
            log.info("Output will be to {}.", this.outFile);
            this.outStream = new FileOutputStream(this.outFile);
        } else {
            log.info("Output will be to the standard output.");
            this.outStream = System.out;
        }
        // Load the coreSEED genome.
        log.info("Reading core genome from {}.", this.coreFile);
        this.coreGenome = new Genome(this.coreFile);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        try (PrintWriter writer = new PrintWriter(this.outStream)) {
            // Create the map of b-numbers to wild-type FIG IDs.
            this.createWildTypeMap();
            // Create the map of CoreSEED IDs to b-numbers.
            this.createCoreMap();
            // Create the map of b-numbers to iModulons.
            this.createModulonMap();
            // Now we read the AR file to produce the output.
            writer.println("fig_id\tmodulon\tregulon\tsubsystems");
            log.info("Reading regulon file {} and producing output.", this.arFile);
            try (TabbedLineReader arStream = new TabbedLineReader(this.arFile)) {
                int count = 0;
                for (TabbedLineReader.Line line : arStream) {
                    String arNum = line.get(0);
                    String figId = line.get(1);
                    String bNum = this.coreBMap.get(figId);
                    if (bNum != null) {
                        List<String> modulons = this.modulonMap.getOrDefault(bNum, Collections.emptyList());
                        String refId = this.bNumFigMap.get(bNum);
                        if (refId != null) {
                            String subList = this.bNumSubMap.getOrDefault(bNum, "");
                            writer.format("%s\t%s\t%s\t%s%n", refId, StringUtils.join(modulons, ','), arNum, subList);
                            count++;
                        }
                    }
                }
                log.info("{} features output.", count);
            }
        } finally {
            if (this.outFile != null)
                this.outStream.close();
        }
    }

    /**
     * This method reads the iModulon file to create a map of b-numbers to iModulon names.
     *
     * @throws IOException
     */
    private void createModulonMap() throws IOException {
        log.info("Processing modulon map.");
        int count = 0;
        try (LineReader modStream = new LineReader(this.modFile)) {
            this.modulonMap = new HashMap<String, List<String>>(NUM_FEATURES);
            // Read the lines as arrays of strings using a comma delimiter.  The NULL
            // parameter means we read to end-of-file.
            for (String[] parts : modStream.new Section(null, ",")) {
                String modulon = parts[0];
                count++;
                for (int i = 1; i < parts.length; i++) {
                    List<String> modulons = this.modulonMap.computeIfAbsent(parts[i], x -> new ArrayList<String>());
                    modulons.add(modulon);
                }
            }
        }
        log.info("{} features found in {} modulons.", this.modulonMap.size(), count);
    }

    /**
     * This method reads the CoreSEED reference genome to create a map of FIG IDs to b-numbers and a map
     * of b-numbers to subsystems.
     *
     * @throws IOException
     */
    private void createCoreMap() throws IOException {
        // Create the output maps.
        this.coreBMap = new HashMap<String, String>(NUM_FEATURES);
        this.bNumSubMap = new HashMap<String, String>(NUM_FEATURES);
        // This map is used to generate subsystem IDs.
        MagicMap<SubsystemRowDescriptor> subMap = new MagicMap<SubsystemRowDescriptor>(new SubsystemRowDescriptor());
        for (Feature feat : this.coreGenome.getPegs()) {
            String bNum = getBNumber(feat);
            if (bNum != null) {
                this.coreBMap.put(feat.getId(), bNum);
                // Now we need to compute the subsystems.
                List<String> subIds = new ArrayList<String>();
                for (SubsystemRow row : feat.getSubsystemRows()) {
                    SubsystemRowDescriptor sub = subMap.getByName(row.getName());
                    if (sub == null)
                        sub = new SubsystemRowDescriptor(row, subMap);
                    subIds.add(sub.getId());
                }
                this.bNumSubMap.put(bNum, StringUtils.join(subIds, ","));
            }
        }
        log.info("{} b-numbers found in {}.", this.coreBMap.size(), coreGenome);
    }

    /**
     * @return the b-number alias of a feature, or NULL if there is none
     *
     * @param feat	feature of interest
     */
    private static String getBNumber(Feature feat) {
        Optional<String> retVal = feat.getAliases().stream().filter(x -> B_NUMBER.matcher(x).matches()).findFirst();
        return (retVal.isPresent() ? retVal.get() : null);
    }

    /**
     * This method reads the wild-type reference genome to create a map of b-numbers to FIG IDs.
     *
     * @throws IOException
     */
    private void createWildTypeMap() throws IOException {
        log.info("Reading wild-type genome from {}.", this.wildFile);
        this.bNumFigMap = new HashMap<String, String>(NUM_FEATURES);
        Genome wildGenome = new Genome(this.wildFile);
        for (Feature feat : wildGenome.getPegs()) {
            String bNum = getBNumber(feat);
            if (bNum != null)
                this.bNumFigMap.put(bNum, feat.getId());
        }
        log.info("{} b-numbers found in {}.", this.bNumFigMap.size(), wildGenome);
    }

}
