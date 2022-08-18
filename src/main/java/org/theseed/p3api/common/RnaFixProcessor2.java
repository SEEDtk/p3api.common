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
import org.apache.commons.lang3.StringUtils;
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
 * of processed RNA Seq data files and a mapping file.  It will copy the data files to an output
 * directory with the strain names corrected.
 *
 * The strain reassignments are almost always a change in host.  If an input file is not in the strain-reassignment list,
 * it is copied unmodified.  If it is in the list and the strain column is blank, it is not copied.  Otherwise, the
 * strain is checked for a change of host, and the IPTG column is checked for an IPTG error.
 *
 * The positional parameters are the name of the input directory, the name of the mapping file, and the name of the
 * output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --clear	erase output directory before processing
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

    /** name of the input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory name")
    private File inDir;

    /** name of the renaming definition file */
    @Argument(index = 1, metaVar = "mapping.tbl", usage = "mapping definition file")
    private File translationFile;

    /** name of the output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory name")
    private File outDir;

    @Override
    protected void setDefaults() {
        this.clearFlag = false;
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
        // First, read in the translations.  For each one, we either map to an empty string or to the updated sample ID.
        // Sample IDs that don't change are discarded.
        log.info("Loading translation map from {}.", this.translationFile);
        var translationMap = new HashMap<String, String>(500);
        try (var tranStream = new TabbedLineReader(this.translationFile)) {
            for (var line : tranStream) {
                var oldSampleId = line.get(0);
                var strainId = line.get(1);
                if (strainId.isBlank())
                    translationMap.put(oldSampleId, "");
                else {
                    boolean keep = false;
                    var sample = (new SampleId(oldSampleId)).normalizeSets();
                    // Update the IPTG.
                    if (! sample.isIPTG() && line.getFlag(2)) {
                        sample.setIptg();
                        keep = true;
                    }
                    // Update the strain.
                    String newHost = StringUtils.substringBefore(strainId, "_");
                    if (! newHost.contentEquals(sample.getFragment(0))) {
                        String newSampleId = sample.replaceFragment(0, newHost);
                        sample = new SampleId(newSampleId);
                        keep = true;
                    }
                    if (keep)
                        translationMap.put(oldSampleId, sample.toString());
                }
            }
        }
        log.info("{} mappings found.", translationMap.size());
        // Now loop through the input directory.
        File[] inFiles = this.inDir.listFiles(File::isFile);
        int skipCount = 0;
        int keepCount = 0;
        int changeCount = 0;
        int copyCount = 0;
        for (File inFile : inFiles) {
            Matcher m = RNA_FILE_NAME.matcher(inFile.getName());
            if (m.matches()) {
                // Get the mapping information.
                String oldSampleId = m.group(1);
                if (oldSampleId.contentEquals("M_0_TA1_C_asdO_000_DtdhDmetlDdapA_0_24_M1"))
                    log.info("Check.");
                String newName;
                if (! oldSampleId.contains("_")) {
                    // NCBI samples are processed without changes.
                    newName = oldSampleId;
                    keepCount++;
                } else {
                    SampleId sample = (new SampleId(oldSampleId)).normalizeSets();
                    String newSampleId = translationMap.get(sample.toString());
                    if (newSampleId != null) {
                        // Here we need to update the sample ID.copy the file unchanged.
                        if (StringUtils.isEmpty(newSampleId)) {
                            // Here the file is bad and is not copied.
                            sample = null;
                            skipCount++;
                        } else {
                            sample = new SampleId(newSampleId);
                            changeCount++;
                        }
                    } else
                        keepCount++;
                    // If the sample is invalid, denote there is no new name; otherwise, normalize the name.
                    if (sample == null)
                        newName = null;
                    else
                        newName = sample.normalizeSets().toString();
                }
                if (newName != null) {
                    File outFile = new File(this.outDir, newName + m.group(2));
                    FileUtils.copyFile(inFile, outFile);
                    log.info("{} copied to {}.", inFile, outFile);
                } else
                    log.info("{} skipped.  No mapping.", inFile);
            }
        }
        log.info("{} copied.  {} unchanged, {} skipped, {} modified.", copyCount, keepCount, skipCount, changeCount);
    }
}
