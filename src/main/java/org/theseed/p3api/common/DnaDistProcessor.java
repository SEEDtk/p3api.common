/**
 *
 */
package org.theseed.p3api.common;

import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.sequence.DnaKmers;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.Sequence;

/**
 * This is a simple command that reads a DNA FASTA file and computes the distances between
 * each pair of sequences.  The maximum distance is output.
 *
 * The positional parameter is the name of the input file.  All output is to the log.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * @author Bruce Parrello
 *
 */
public class DnaDistProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DnaDistProcessor.class);

    // COMMAND-LINE OPTIONS

    @Argument(index = 0, metaVar = "input.fna", usage = "FASTA file of DNA sequences")
    private File inFile;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.inFile.canRead())
            throw new FileNotFoundException("Input file " + this.inFile + " is not found or unreadable.");
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        List<Sequence> seqs = FastaInputStream.readAll(this.inFile);
        List<DnaKmers> kmers = seqs.stream().map(x -> new DnaKmers(x.getSequence()))
                .collect(Collectors.toList());
        log.info("{} sequences read.", seqs.size());
        double maxDist = 0.0;
        for (int i = 0; i < kmers.size(); i++) {
            DnaKmers seqK = kmers.get(i);
            String label = seqs.get(i).getLabel();
            for (int j = i + 1; j < seqs.size(); j++) {
                double dist = seqK.distance(kmers.get(j));
                if (dist > maxDist) maxDist = dist;
                log.info("Distance from {} to {} is {}.", label, seqs.get(j).getLabel(), dist);
            }
        }
        log.info("Maximum distance is {}.", maxDist);
    }

}
