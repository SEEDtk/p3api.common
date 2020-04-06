/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.Connection;
import org.theseed.p3api.P3Genome;
import org.theseed.utils.BaseProcessor;

/**
 * This class downloads multiple GTOs specified by an input file.  The input file must be tab-delimited,
 * with headers.  The default detail level is FULL.
 *
 * The positional parameter is the name of the output directory.  The following command-line options are
 * supported.
 *
 * -h	display usage information
 * -v	display more detailed progress on the log
 * -i	input file name; the default is STDIN
 * -c	index (1-based) or name of the input column; the default is "genome_id"
 * -d	detail level for the GTOs; the default is FULL
 * -m	only download GTOs not already in the directory
 *
 * --clear	erase the output directory before starting
 *
 * @author Bruce Parrello
 *
 */
public class GtoProcessor extends BaseProcessor {

    // FIELDS

    /** input stream */
    private TabbedLineReader inStream;

    /** input column index */
    private int idIdx;


    // COMMAND-LINE OPTIONS

    /** input column identifier */
    @Option(name = "-c", aliases = { "--col", "--column" }, metaVar = "1", usage = "input column containing genome IDs")
    private String colName;

    /** detail level */
    @Option(name = "-d", aliases = { "--level", "--details" }, usage = "detail level for the GTOs")
    private P3Genome.Details level;

    /** input file (if not STDIN) */
    @Option(name = "-i", aliases = { "--input" }, metaVar = "inFile", usage = "name of input file (default is to use STDIN)")
    private File inFile;

    /** missing-only flag */
    @Option(name = "-m", aliases = { "--safe", "--missing" }, usage = "if specified, GTOs already in the directory will not be overwritten")
    private boolean missingFlag;

    /** clear-output flag */
    @Option(name = "--clear", usage = "clear output directory before downloading")
    private boolean clearFlag;

    /** output directory name */
    @Argument(index = 0, metaVar = "outDir", usage = "output directory name", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.colName = "genome_id";
        this.level = P3Genome.Details.FULL;
        this.inFile = null;
    }

    @Override
    protected boolean validateParms() throws IOException {
        if (this.inFile == null) {
            log.info("Genome IDs will be read from standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else if (! this.inFile.canRead())
            throw new FileNotFoundException("Input file " + this.inFile + " not found or unreadable.");
        else {
            log.info("Genome IDs will be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            if (! this.outDir.mkdirs())
                throw new IOException("Could not create output directory " + this.outDir + ".");
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else {
            log.info("Output will be to directory {}.", this.outDir);
        }
        log.info("GTO detail level is {}.", this.level);
        // Compute the input column index.
        this.idIdx = this.inStream.findField(this.colName);
        log.info("Incoming genome IDs will be taken from column {}.", this.inStream.getLabels()[this.idIdx]);
        return true;
    }

    @Override
    public void run() {
        try {
            int outputCount = 0;
            log.info("Connecting to PATRIC.");
            Connection p3 = new Connection();
            for (TabbedLineReader.Line line : this.inStream) {
                String genomeId = line.get(this.idIdx);
                // Compute the output file name.
                File outFile = new File(this.outDir, genomeId + ".gto");
                if (outFile.exists() && this.missingFlag)
                    log.info("Skipping genome {}: file {} already present.", genomeId, outFile);
                else {
                    P3Genome genome = P3Genome.Load(p3, genomeId, this.level);
                    if (genome == null)
                        log.warn("Genome {} not found in PATRIC.");
                    else {
                        log.info("Saving {} to output.", genome);
                        genome.update(outFile);
                        outputCount++;
                    }
                }
            }
            log.info("All done. {} GTOs output to {}.", outputCount, this.outDir);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.inStream.close();
        }

    }

}
