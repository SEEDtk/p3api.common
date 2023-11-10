/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.genome.GenomeMultiDirectory;
import org.theseed.genome.iterator.GenomeSource;

/**
 * Copy a set of genomes to a genome master directory.  Note that if the input is PATRIC, the output directory will
 * contain full genomes and the job is not restartable, so this is a bad idea for a large genome set.
 *
 * The positional parameters are the name of the input genome source and the name of the output directory.
 * If the output directory already exists, it will be updated.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --clear		erase the output directory before starting
 * --source		type of input (master directory, GTO directory, PATRIC ID file); default is GTO directory
 * --missing	only copy a genome if it is not already in the output directory
 *
 * @author Bruce Parrello
 *
 */
public class CopyMasterProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CopyMasterProcessor.class);
    /** input genomes */
    private GenomeSource genomes;
    /** output master directory */
    private GenomeMultiDirectory master;

    /** TRUE to erase any old genomes */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before starting")
    private boolean clearFlag;

    /** TRUE to only copy new genomes */
    @Option(name = "--missing", usage = "if specified, input genomes not overwrite existing genomes")
    private boolean missingFlag;

    /** type of input */
    @Option(name = "--source", usage = "type of genome input (master directory, normal, PATRIC ID file)")
    private GenomeSource.Type inType;

    /** input genome file or directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input genomes (file or directory)", required = true)
    private File inDir;

    /** output master directory */
    @Argument(index = 1, metaVar = "outDir", usage = "output genome master directory", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.inType = GenomeSource.Type.DIR;
        this.clearFlag = false;
        this.missingFlag = false;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Create the source stream.
        this.genomes = this.inType.create(this.inDir);
        // Create the output directory.
        if (this.clearFlag)
            this.master = GenomeMultiDirectory.create(this.outDir, true);
        else
            this.master = new GenomeMultiDirectory(this.outDir);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        long count = 0;
        long total = this.genomes.size();
        long start = System.currentTimeMillis();
        for (Genome genome : this.genomes) {
            if (! this.missingFlag || ! this.master.contains(genome.getId()))
                this.master.add(genome);
            count++;
            Duration speed = Duration.ofMillis(System.currentTimeMillis() - start).dividedBy(count);
            log.info("{} of {} processed; {} per genome.", count, total, speed.toString());
        }
    }

}
