/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a quick program to generate test data files for the RNA Seq Group-File formatter.  The
 * output files will be called groupTestXX.tbl in the output directory, where "XX" is a sequence
 * number.  The positional parameters should be the input GTO file name and the output directory
 * name.
 *
 * The algorithm will be simple, creating groups one at a time, assigning the features sequentially.
 *
 * The command-line options are be as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --size		max number of features per group (default 10)
 * --num		number of groups per type (default 100)
 * --limit		number of groups per file (default 75)
 * --types		comma-delimited list of types (default "modulon,regulon,operon")
 * --clear		erase the output directory before processing
 *
 * @author Bruce Parrello
 *
 */
public class GroupTestProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GroupTestProcessor.class);
    /** set of group types */
    private Set<String> types;
    /** random number generator */
    private Random randomizer;
    /** list of eligible genome features */
    private List<Feature> flist;
    /** current feature iterator */
    private Iterator<Feature> iter;
    /** next output file number */
    private int fileCounter;
    /** number of groups in the current file */
    private int fileContentCounter;
    /** output writer for the current file */
    private PrintWriter writer;

    // COMMAND-LINE OPTIONS

    /** max number of features per group */
    @Option(name = "--size", metaVar = "5", usage = "maximum number of features to put in a group")
    private int groupSize;

    /** number of groups to create of each type */
    @Option(name = "--num", metaVar = "75", usage = "number of groups to create for each group type")
    private int numGroups;

    /** maximum number of groups to put in each output file */
    @Option(name = "--limit", metaVar = "50", usage = "maximum number of groups to put in each output file")
    private int fileLimit;

    /** comma-delimited list of group types */
    @Option(name = "--types", metaVar = "cluster,modulon", usage = "comma-delimited list of group types")
    private String typeNames;

    /** if specified, the output directory will be cleared before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** file name of the source genome */
    @Argument(index = 0, metaVar = "input.gto", usage = "name of the source genome GTO file", required = true)
    private File inFile;

    /** name of the output directory */
    @Argument(index = 1, metaVar = "outDir", usage = "name of the output directory", required = true)
    private File outDir;


    @Override
    protected void setDefaults() {
        this.groupSize = 10;
        this.numGroups = 100;
        this.fileLimit = 75;
        this.typeNames = "operon,modulon,regulon";
        this.clearFlag = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (this.groupSize < 1)
            throw new ParseFailureException("Maximum group size must be at least 1.");
        if (this.numGroups < 1)
            throw new ParseFailureException("Number of groups per type must be at least 1.");
        if (this.fileLimit < 1)
            throw new ParseFailureException("Number of groups per file must be at least 1.");
        // Create the type name set.
        if (StringUtils.containsAny(this.typeNames, " \t\n\r"))
            throw new ParseFailureException("Illegal use of whitespace in type name string.");
        this.types = Set.of(StringUtils.split(this.typeNames, ','));
        if (this.types.size() < 1)
            throw new ParseFailureException("Type name string cannot be empty.");
        if (this.types.stream().anyMatch(x -> x.isBlank()))
            throw new ParseFailureException("Type names must be nonblank.");
        // Load the input genome.
        if (! this.inFile.canRead())
            throw new FileNotFoundException("Input genome file " + this.inFile + " is not found or unreadable.");
        Genome genome = new Genome(this.inFile);
        // Get a list of all the pegs and set up to iterate through it.
        this.flist = genome.getPegs();
        Collections.sort(this.flist);
        this.iter = this.flist.iterator();
        log.info("Source genome is {} with {} pegs.", genome, this.flist.size());
        // Now set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output files will be created in directory {}.", this.outDir);
        // Initialize for the first output file.
        this.fileCounter = 1;
        this.writer = null;
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        try {
            // Initialize the randomizer.
            this.randomizer = new Random();
            // Create the first output file.
            this.startNewFile();
            // Create a buffer to hold feature specifiers.
            List<String> specs = new ArrayList<String>(this.groupSize);
            // Loop through the group types.
            for (String type : this.types) {
                // Create the prefix string for this group type.
                String prefix = this.groupPrefix(type);
                log.info("Prefix for type {} is {}.", type, prefix);
                // Loop through the groups for this type.
                for (int grpNum = 0; grpNum < this.numGroups; grpNum++) {
                    // Create this group's name.
                    String grpName = String.format("%s%04x", prefix, grpNum);
                    // Compute its size.
                    int nFeats = this.randomizer.nextInt(this.groupSize) + 1;
                    // Now we need to fill the group.  Get the desired list of features.
                    specs.clear();
                    IntStream.range(0, nFeats).forEach(i -> specs.add(this.nextFeat()));
                    // Insure there room in the current file.
                    if (this.fileContentCounter >= this.fileLimit)
                        this.startNewFile();
                    // Write out the group.
                    this.writer.println(type + "\t" + grpName + "\t" + StringUtils.join(specs, ", "));
                    this.fileContentCounter++;
                }
            }
        } finally {
            // Insure we've closed the current output file.
            if (this.writer != null)
                this.writer.close();
        }
    }

    /**
     * Set up the next output file.
     *
     * @throws FileNotFoundException
     */
    private void startNewFile() throws FileNotFoundException {
        // Insure the old file is closed.
        if (this.writer != null)
            this.writer.close();
        this.writer = null;
        // Create the new file name.
        File newFile = new File(this.outDir, String.format("groupTest%02d.tbl", this.fileCounter));
        this.fileCounter++;
        // Denote it is empty.
        this.fileContentCounter = 0;
        // Open it for output and write the header record.
        log.info("Opening new file {} for output.", newFile);
        this.writer = new PrintWriter(newFile);
        this.writer.println("type\tname\tfeatures");
    }

    /**
     * @return a suitable prefix for this group type
     *
     * @param type	group type name
     */
    private String groupPrefix(String type) {
        StringBuilder buffer = new StringBuilder(type.length());
        String typeUC = type.toUpperCase();
        // The first letter always goes in.
        buffer.append(typeUC.charAt(0));
        // Loop through the rest, removing vowels and non-letters.
        for (int i = 1; i < type.length(); i++) {
            char c = typeUC.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                switch (c) {
                case 'A' :
                case 'E' :
                case 'I' :
                case 'O' :
                case 'U' :
                    break;
                default :
                    buffer.append(c);
                }
            }
        }
        return buffer.toString();
    }


    /**
     * Get the next feature to output.  If the iterator is at the end, we circle back to the first feature.
     *
     * @return a specifier for the next feature
     */
    private String nextFeat() {
        if (! this.iter.hasNext())
            this.iter = this.flist.iterator();
        // Now we get the feature descriptor.
        Feature feat = this.iter.next();
        // We need choose a random alias.  It can be the full ID, the suffix, or a provided alias.
        String retVal;
        var aliases = new ArrayList<String>(feat.getAliases());
        int choices = aliases.size();
        int choice = this.randomizer.nextInt(choices + 2);
        if (choice == choices) {
            // Use the suffix.
            retVal = "peg." + StringUtils.substringAfterLast(feat.getId(), ".");
        } else if (choice > choices) {
            // Use the full ID.
            retVal = feat.getId();
        } else {
            // Use the indicated alias.
            retVal = aliases.get(choice);
        }
        return retVal;
    }

}
