/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.samples.SampleId;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command sets up to fix RNA Seq file names for the threonine project.  It takes as input a directory
 * of processed RNA Seq data files and a strain re-assignment file.  It will copy the data files to an output
 * directory with the strain names corrected.
 *
 * The strain reassignments are always a change in host.  Each reassignment entry has an old-name chromosome definition,
 * the old host, and the new host.  "926" is "M" and "277" is "7".  "nrrl XXXXX" hosts are converted to simply "XXXXX".
 * The basic strategy is to convert each reassignment chromosome to a sample ID and map the old chromosome string to
 * the new host.  If an incoming file name's sample ID matches at the chromosome level, we copy the file, renaming it if
 * the host has changed.
 *
 * The positional parameters are the name of the input directory, the name of the renaming file, and the name of the
 * output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --clear	erase output directory before processing
 * --iptg	force IPTG on in output directory
 *
 * @author Bruce Parrello
 *
 */
public class RnaFixProcessor2 extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaFixProcessor2.class);
    /** match patter for file names */
    protected static final Pattern RNA_FILE_NAME = Pattern.compile("(.+)(\\.samstat\\.html|_genes\\.fpkm)");

    // COMMAND-LINE OPTIONS

    /** TRUE to clear the output directory before beginning */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** TRUE to force IPTG on in the output directory */
    @Option(name = "--iptg", usage = "if specified, IPTG will be forced on in the output directory")
    private boolean iptgFlag;

    /** name of the input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory name")
    private File inDir;

    /** name of the renaming definition file */
    @Argument(index = 1, metaVar = "new_strains.txt", usage = "renaming definition file")
    private File translationFile;

    /** name of the output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory name")
    private File outDir;

    @Override
    protected void setDefaults() {
        this.clearFlag = false;
        this.iptgFlag = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Insure the input directory exists.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        // Insure we can read the translation file.
        if (! this.translationFile.canRead())
            throw new FileNotFoundException("Translation file " + this.translationFile + " is not found or unreadable.");
        // Prepare the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output will be to directory {}.", this.outDir);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // First, read in the translations.  The map translates a chromosome string to a new host name.  A host name
        // of "?" means the chromosome's samples should be discarded.
        log.info("Loading translation map from {}.", this.translationFile);
        var translationMap = new HashMap<String, String>(500);
        try (var tranStream = new TabbedLineReader(this.translationFile)) {
            for (var line : tranStream) {
                // We need to convert hyphens to "x" in the chromosome definition because of an old amibiguity problem.
                String chromeDef = line.get(0).replace('-', 'x');
                // Create a sample ID from the chromosome definition.
                SampleId sample = SampleId.translate(chromeDef, 24.0, true, "M1").normalizeSets();
                String chromosome = sample.toChromosome();
                // Translate the target host.
                var target = line.get(2);
                if (target.startsWith("nrrl "))
                    target = target.substring(5);
                else if (target.contentEquals("277"))
                    target = "7";
                else if (target.startsWith("?"))
                    target = "?";
                else
                    target = "M";
                // Store the mapping.
                translationMap.put(chromosome, target);
            }
        }
        log.info("{} mappings found.", translationMap.size());
        // Now loop through the input directory.
        File[] inFiles = this.inDir.listFiles(File::isFile);
        for (File inFile : inFiles) {
            Matcher m = RNA_FILE_NAME.matcher(inFile.getName());
            if (m.matches()) {
                String sampleId = m.group(1);
                // If this is an NCBI sample, copy it unmodified.
                File outFile;
                if (! sampleId.contains("_"))
                    outFile = new File(this.outDir, inFile.getName());
                else {
                    SampleId sample = new SampleId(m.group(1));
                    if (this.iptgFlag)
                        sample.setIptg();
                    // Compute the new sample name.
                    String chrome = sample.toChromosome();
                    String target = translationMap.getOrDefault(chrome, "?");
                    if (! target.contentEquals("?")) {
                        String newName = sample.replaceFragment(SampleId.STRAIN_COL, target);
                        outFile = new File(this.outDir, newName + m.group(2));
                    } else {
                        outFile = null;
                    }
                }
                if (outFile != null) {
                    FileUtils.copyFile(inFile, outFile);
                    log.info("{} copied to {}.", inFile, outFile);
                }
            }
        }
    }

}
