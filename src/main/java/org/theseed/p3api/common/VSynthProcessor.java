/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.sequence.Sequence;
import org.theseed.utils.FloatList;

/**
 * This command reads single-contig FASTA files from a directory and combines them into a single file.  The
 * output file will contain each full contig and then various truncated and combined contigs to test
 * contamination and completeness results for single-contig genomes.
 *
 * The algorithm is to generate 2N contigs for each input contig.  The first is a truncated version of
 * the input contig at the specified completeness fraction.  The second is a combined contig with a
 * random fractional contig from another genome.  The list of completeness fractions to use is given
 * by a command-line option.
 *
 * The positional parameters are the name of the input directory.  The output FASTA will be produced on the
 * standard output.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v 	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --complete	completeness fractions to test for, comma-delimited-- default 0.8,0.5,0.3
 *
 * @author Bruce Parrello
 *
 */
public class VSynthProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(VSynthProcessor.class);
    /** list of completeness fractions */
    private FloatList fractions;
    /** map of incoming contig IDs to uncontaminated sequences generated */
    private Map<String, List<Sequence>> contigMap;
    /** random-number generator */
    private Random randomizer;
    /** output stream */
    private FastaOutputStream outStream;
    /** name pattern for FASTA files */
    private static final Pattern FASTA_PATTERN = Pattern.compile(".+\\.(?:fna|fa|fasta)");
    /** file filter for FASTA files */
    private static final FileFilter FASTA_FILTER = new FileFilter() {

        @Override
        public boolean accept(File pathname) {
            boolean retVal = false;
            if (pathname.canRead() && pathname.isFile()) {
                Matcher m = FASTA_PATTERN.matcher(pathname.getName());
                retVal = m.matches();
            }
            return retVal;
        }

    };

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "output.fna", usage = "output file (if not STDOUT)")
    private File outFile;

    /** list of completeness fractions */
    @Option(name = "--complete", usage = "comma-delimited list of completeness fractions")
    private void setComplete(String completeString) {
        this.fractions = new FloatList(completeString);
    }

    /** input directory */
    @Argument(index = 0, metaVar = "inDir", usage = "input directory containing FASTA files")
    private File inDir;

    @Override
    protected void setDefaults() {
        this.outFile = null;
        this.outStream = null;
        this.fractions = new FloatList(0.8, 0.5, 0.3);
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Initialize the randomizer.
        this.randomizer = new Random();
        // Verify that all the fractions are in range.
        for (double fract : this.fractions) {
            if (fract <= 0.0 || fract >= 1.0)
                throw new ParseFailureException("Completeness fractions must be strictly between 0 and 1.");
        }
        // Verify the input directory.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        // Get the incoming FASTA files.
        File[] inFiles = this.inDir.listFiles(FASTA_FILTER);
        if (inFiles.length <= 0)
            throw new FileNotFoundException("No readable FASTA files found in " + this.inDir + ".");
        // Compute the number of sequences that will be generated for each file.
        int listSize = this.fractions.size() * 2 + 1;
        // Now read them into memory, giving each one a space in the main hash.
        this.contigMap = new HashMap<String, List<Sequence>>(inFiles.length * 3 / 2 + 1);
        log.info("Reading {} single-contig FASTA files.", inFiles.length);
        for (File inFile : inFiles) {
            try (FastaInputStream inStream = new FastaInputStream(inFile)) {
                log.info("Processing input file {}.", inFile);
                if (! inStream.hasNext())
                    throw new FileNotFoundException("No sequences found in file " + inFile + ".");
                Sequence inSeq = inStream.next();
                String seqName = StringUtils.substringBeforeLast(inFile.getName(), ".");
                if (this.contigMap.containsKey(seqName))
                    throw new IOException("File " + inFile + " has a duplicate name.  The un-suffixed name of each file must be unique.");
                // Create the label for a full sequence.
                inSeq.setLabel(seqName + ".full");
                List<Sequence> seqList = new ArrayList<Sequence>(listSize);
                seqList.add(inSeq);
                this.contigMap.put(seqName, seqList);
            }
        }
        // Finally, set up the output stream.
        if (this.outFile == null) {
            log.info("Output will be to the standard output.");
            this.outStream = new FastaOutputStream(System.out);
        } else {
            log.info("Output will be to the file {}.", this.outFile);
            this.outStream = new FastaOutputStream(this.outFile);
        }
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        try {
            // This list will contain the previous fractional sequences.  We pull our contamination from here.
            List<Sequence> oldSeqs = new ArrayList<Sequence>(this.contigMap.size() * 2 * this.fractions.size());
            int contamSeqs = 0;
            // Loop through the input files, processing each one.
            for (var seqEntry : this.contigMap.entrySet()) {
                String seqName = seqEntry.getKey();
                List<Sequence> seqs = seqEntry.getValue();
                // Get the original sequence.
                Sequence original = seqs.get(0);
                String oldSeq = original.getSequence();
                int len = original.length();
                StringBuffer seqBuffer = new StringBuffer(len);
                log.info("Processing sequence {} with length {}.", seqName, len);
                // We will buffer fractional sequences in here.
                List<Sequence> fractionals = new ArrayList<Sequence>(this.fractions.size());
                // Loop through the fractions.
                for (double frac : this.fractions) {
                    // Compute the desired fractional length.  Because we are truncating, it will be strictly
                    // less than the full length.
                    int fracLen = (int) (len * frac);
                    if (fracLen > 0) {
                        // Clear the output buffer.
                        seqBuffer.setLength(0);
                        // Create the new sequence name.
                        String fracName = seqName + ".frac." + String.valueOf(frac);
                        // Create the fractional sequence.
                        int removeLen = len - fracLen;
                        int start = this.randomizer.nextInt(fracLen);
                        if (start > 0)
                            seqBuffer.append(StringUtils.substring(oldSeq, 0, start));
                        seqBuffer.append(StringUtils.substring(oldSeq, start + removeLen));
                        // Build the new sequence object.
                        String newSeq = seqBuffer.toString();
                        log.debug("Sequence {} has length {}.", fracName, newSeq.length());
                        Sequence seq = new Sequence(fracName, "", newSeq);
                        // Save it in the lists.
                        seqs.add(seq);
                        fractionals.add(seq);
                        // Now we want to append a random contamination sequence, if there is one available.
                        if (! oldSeqs.isEmpty()) {
                            int randIdx = this.randomizer.nextInt(oldSeqs.size());
                            Sequence contaminator = oldSeqs.get(randIdx);
                            String contamLabel = fracName + "." + contaminator.getLabel();
                            String contamSeq = newSeq + contaminator.getSequence();
                            Sequence contaminated = new Sequence(contamLabel, "", contamSeq);
                            seqs.add(contaminated);
                            contamSeqs++;
                        }
                    }
                }
                // Add the fractional sequences to the old-sequence list.
                oldSeqs.addAll(fractionals);
            }
            log.info("{} fractional sequences and {} contaminated sequences created.", oldSeqs.size(), contamSeqs);
            // Now write everything out.
            int seqsOut = 0;
            for (List<Sequence> seqList : this.contigMap.values()) {
                this.outStream.write(seqList);
                seqsOut += seqList.size();
            }
            log.info("{} sequences written.", seqsOut);
        } finally {
            if (this.outStream != null)
                this.outStream.close();
        }

    }

}
