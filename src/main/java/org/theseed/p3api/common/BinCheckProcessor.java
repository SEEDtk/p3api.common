package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3Connection;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.sequence.Sequence;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command reads a binning reference-genome FASTA file and removes genomes that
 * have been deleted from PATRIC.
 *
 * The positional parameters are the names of the input and output FASTA files.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * @author Bruce Parrello
 *
 */
public class BinCheckProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BinCheckProcessor.class);

    // COMMAND-LINE OPTIONS

    /** input FASTA file */
    @Argument(index = 0, metaVar = "inFile.fa", usage = "input FASTA file", required = true)
    private File inFile;

    /** output FASTA file */
    @Argument(index = 1, metaVar = "outFile.fa", usage = "output FASTA file", required = true)
    private File outFile;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Insure the input file exists.
        if (! this.inFile.canRead())
            throw new FileNotFoundException("Input FASTA " + this.inFile + " is not found or unreadable.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        /** Get the set of genomes currently in PATRIC. */
        Set<String> goodGenomes = this.getGoodGenomes();
        log.info("{} genomes found in PATRIC.", goodGenomes.size());
        int deleteCount = 0;
        int readCount = 0;
        // Open the two fasta files.
        try (var inStream = new FastaInputStream(this.inFile);
                var outStream = new FastaOutputStream(this.outFile)) {
            for (Sequence inSeq : inStream) {
                readCount++;
                String genome = StringUtils.substringBefore(inSeq.getComment(), "\t");
                if (! goodGenomes.contains(genome)) {
                    log.info("Deleting obsolete genome {}.", inSeq.getComment());
                    deleteCount++;
                } else
                    outStream.write(inSeq);
            }
        }
        log.info("{} genomes deleted.  {} read.", deleteCount, readCount);
    }

    /**
     * @return the set of genome IDs from PATRIC
     */
    private Set<String> getGoodGenomes() {
        // Get a list of all the prokaryotic genomes.
        var p3 = new P3Connection();
        List<JsonObject> genomes = new ArrayList<JsonObject>(500000);
        p3.addAllProkaryotes(genomes);
        // Create a set of the genome IDs.
        var retVal = genomes.stream().map(x -> KeyBuffer.getString(x, "genome_id"))
                .collect(Collectors.toSet());
        return retVal;
    }

}
