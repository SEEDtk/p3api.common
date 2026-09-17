package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.Sequence;

/**
 * This is a very simple command that computes the total and average length of the sequences in a FASTA file.
 * The FASTA file should be on the standard input. The results will be written to the log. The command-line
 * options are as follows:
 * 
 * -h       display command-line usage
 * -v       display more frequent log messages
 * -i       input FASTA file (default is standard input)
 * 
 */
public class FastaLenProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FastaLenProcessor.class);
    /** statistical summary of the FASTA sequence lengths */
    private SummaryStatistics stats;

    // COMMAND-LINE OPTIONS

    @Option(name = "--input", aliases = { "-i" }, metaVar = "contigs.fasta", usage = "input FASTA file name (if not specified, standard input is used)")
    private File inFile;

    @Override
    protected void setDefaults() {
        this.inFile = null;  // default is standard input
    }

    @Override
    protected void validateParms() throws IOException, ParseFailureException {
        if (this.inFile != null && ! this.inFile.canRead())
            throw new IOException("Cannot read input file " + this.inFile);
        // Set up the statistics object.
        this.stats = new SummaryStatistics();
    }

    @Override
    protected void runCommand() throws Exception {
        log.debug("Opening FASTA file.");
        try (FastaInputStream fastaStream = this.openInput()) {
            for (Sequence seq : fastaStream) {
                this.stats.addValue(seq.length());
            }
        }
        double megabytes = Math.round(this.stats.getSum() / 100_000.0) / 10.0;
        log.info("Total sequences: {}", this.stats.getN());
        log.info("Total length: {} ({} MB)", this.stats.getSum(), megabytes);
        log.info("Average length: {}", this.stats.getMean());
    }

    /**
     * Open the input FASTA file (or standard input if no file is specified) and return a FastaInputStream.
     * 
     * @return a FastaInputStream for the input FASTA file or standard input
     * 
     * @throws IOException if the input file cannot be read or opened
     */
    private FastaInputStream openInput() throws IOException {
        FastaInputStream retVal;
        if (this.inFile == null) {
            retVal = new FastaInputStream(System.in);
            log.info("Sequences will be read from standard input.");
        } else {
            retVal = new FastaInputStream(this.inFile);
            log.info("Sequences will be read from file {}.", this.inFile);
        }
        return retVal;
    }

}


    
