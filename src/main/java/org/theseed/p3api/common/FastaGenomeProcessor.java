/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Contig;
import org.theseed.genome.Genome;
import org.theseed.sequence.FastaInputStream;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command loads the contigs for a genome from a FASTA file.  The contig ID will be the sequence label, the description
 * the sequence comment, and the sequence the sequence DNA.  The DNA will be normalized to lower-case.  In addition, long
 * sequences of "n" and/or "x" will be removed.  The contig will be split at that point and the contig ID modified with a
 * sequential modifier.  (That is, the first fragment will have a suffix of "_1", the second a suffix of "_2", and so forth.)
 *
 * The incoming GTO will be loaded and all its subsystems, features, and contigs deleted.  The contigs will then be replaced
 * by the new contigs, and the resulting genome written back to the original file.
 *
 * The positional parameters are the name of the GTO file and the name of the FASTA file.  The command-line options are as
 * follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -n	number of N/X nucleotides to trigger a scaffold break (default 80)
 *
 * @author Bruce Parrello
 *
 */
public class FastaGenomeProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FastaGenomeProcessor.class);
    /** input genome */
    private Genome genome;


    // COMMAND-LINE OPTIONS

    /** minimum number of ambiguity characters to trigger a scaffold break */
    @Option(name = "--nSize", aliases = { "-n" }, metaVar = "100", usage = "number of N/X characters to trigger a contig break")
    private int scaffoldSize;

    /** genome GTO file name */
    @Argument(index = 0, metaVar = "genome.gto", usage = "input genome GTO file", required = true)
    private File genomeFile;

    /** contig FASTA file name */
    @Argument(index = 1, metaVar = "contigs.fasta", usage = "input contig FASTA file", required = true)
    private File fastaFile;

    @Override
    protected void setDefaults() {
        this.scaffoldSize = 80;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Create the scaffold string.  We search for this to find a break.
        if (this.scaffoldSize < 2)
            throw new ParseFailureException("N size must be at least 2.");
        Pattern.compile(String.format("(.*?)[nx]{%d,}", this.scaffoldSize));
        // Validate the FASTA file.
        if (! this.fastaFile.canRead())
            throw new FileNotFoundException("FASTA input file " + this.fastaFile + " is not found or unreadable.");
        // Load the input genome.
        if (! this.genomeFile.canRead())
            throw new FileNotFoundException("Genome input file " + this.genomeFile + " is not found or unreadable.");
        this.genome = new Genome(this.genomeFile);
        log.info("{} loaded from {}.", this.genome, this.genomeFile);
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        log.info("Clearing old data from {}.", this.genome);
        this.genome.clear();
        // Loop through the FASTA file.
        try (FastaInputStream inStream = new FastaInputStream(this.fastaFile)) {
            log.info("Reading new contigs.");
            for (var contigSeq : inStream) {
                // Get the label and comment.
                String contigId = contigSeq.getLabel();
                String description = contigSeq.getComment();
                // Normalize the sequence.
                String dna = contigSeq.getSequence().toLowerCase();
                final int len = dna.length();
                // Now we split the sequence using scaffold breaks.  This contains the number of
                // fragments output.
                int fragIdx = 0;
                // This is our current offset into the contig.
                int pos = 0;
                // This is the current fragment being built.
                StringBuilder fragment = new StringBuilder(len);
                // Finally, this is the number of N/X characters in sequence preceding this position.
                int nCount = 0;
                while (pos < len) {
                    final char ch = dna.charAt(pos);
                    switch (ch) {
                    case 'x' :
                    case 'n' :
                        // Here we have an ambiguity character.
                        nCount++;
                        if (nCount >= this.scaffoldSize && fragment.length() > 0) {
                            // We have found a scaffold break and we have a fragment to output.
                            fragIdx++;
                            this.addFragment(contigId, description, pos, fragIdx, fragment.toString());
                            // Set up for the next fragment.
                            fragment.setLength(0);
                        }
                        break;
                    default :
                        // Here we have a real character.
                        if (nCount > 0) {
                            // Here we have some ambiguity characters to handle.
                            if (nCount < this.scaffoldSize) {
                                // They weren't a scaffold break.  Add them to the fragment in progress.
                                while (nCount > 0) {
                                    fragment.append('n');
                                    nCount--;
                                }
                            } else {
                                // They were a scaffold break.  Ignore them.
                                nCount = 0;
                            }
                        }
                        fragment.append(ch);
                    }
                    // Update the position.
                    pos++;
                }
                // Check for a residual.
                if (fragment.length() > 0) {
                    // Here we have a residual.  Were there any fragments?
                    if (fragIdx == 0) {
                        // No.  Write the whole sequence as a contig.
                        Contig contig = new Contig(contigId, dna, this.genome.getGeneticCode());
                        contig.setDescription(description);
                        log.info("Contig {} written in a single fragment.", contigId);
                    } else {
                        // Yes.  Write the final fragment.  If there are trailing Ns, add them here.
                        if (nCount > 0)
                            fragment.append("n".repeat(nCount));
                        fragIdx++;
                        this.addFragment(contigId, description, pos, fragIdx, fragment.toString());
                    }
                }
                if (fragIdx > 0)
                    log.info("Contig {} split into {} fragments.", contigId, fragIdx);
            }
        }
        // Now, save the updated GTO.
        log.info("Writing updated genome.");
        this.genome.save(this.genomeFile);
    }

    /**
     * Add a contig fragment to the genome.
     *
     * @param contigId		contig ID from the FASTA label
     * @param description	contig description from the FASTA comment
     * @param pos			position of the fragment in the contig
     * @param fragIdx		ordinal number of the fragment in the contig
     * @param fragmentSeq	DNA sequence of the fragment
     */
    public void addFragment(String contigId, String description, int pos, int fragIdx, String fragmentSeq) {
        Contig fragContig = new Contig(String.format("%s_%d", contigId, pos + 1), fragmentSeq, this.genome.getGeneticCode());
        fragContig.setDescription(String.format("%s fragment %d", description, fragIdx));
        this.genome.addContig(fragContig);
    }

}
