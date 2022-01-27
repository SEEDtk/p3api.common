/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.Sequence;
import org.theseed.utils.BasePipeProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads the output from a hammer-binning run using synthetic data and produces an xmatrix from
 * the results.  The final "type" column will be "Good" if the correct bin was chosen, "Bad" if the wrong bin was
 * chosen, and "None" if no bin was chosen.
 *
 * The original FASTA files should be in a single directory.  We will scan them to compute the sequence
 * lengths.
 *
 * The positional parameter is the name of the directory containing the FASTA files.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -i	input file containing the hammer hit counts (tab-delimited with headers)
 * -o	output file for the xmatrix
 * -d	minimum difference required to constitute a hit (default 1)
 *
 *
 * @author Bruce Parrello
 *
 */
public class HammerTestProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerTestProcessor.class);
    /** map of contig IDs to lengths */
    private Map<String, Integer> lenMap;
    /** number of hits expected in an input line */
    private final static int HIT_VALUES = 4;
    /** pattern for parsing contig ID */
    private final Pattern CONTIG_ID_PATTERN = Pattern.compile("REP_(\\d+\\.\\d+)_\\d+_covg_([0-9.]+)");

    // COMMAND-LINE OPTIONS

    /** minimum hit distance */
    @Option(name = "--diff", aliases = { "-d" }, metaVar = "4", usage = "minimum hit count difference required for a good hit")
    private int minDiff;

    /** name of the directory containing the FASTA files */
    @Argument(index = 0, metaVar = "fastaDir", usage = "directory containing the FASTA files", required = true)
    private File inDir;

    @Override
    protected void setPipeDefaults() {
        this.minDiff = 1;
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the tuning parameters.
        if (this.minDiff <= 0)
            throw new ParseFailureException("Minimum difference must be positive.");
        // Process the FASTA input directory.
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input FASTA directory " + this.inDir + " is not found or invalid.");
        // Get all the FASTA files in the directory.
        File[] fastaFiles = this.inDir.listFiles(new FastaInputStream.Fasta());
        log.info("{} fasta files found in input directory.", fastaFiles.length);
        this.lenMap = new HashMap<String, Integer>(fastaFiles.length * 1300);
        for (File fastaFile : fastaFiles) {
            log.info("Scanning {}.", fastaFile);
            try (FastaInputStream fastaStream = new FastaInputStream(fastaFile)) {
                for (Sequence seq : fastaStream)
                    this.lenMap.put(seq.getLabel(), seq.length());
            }
        }
        log.info("{} sequence lengths stored.", this.lenMap.size());
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // We will use this array to hold the hit counts.
        int[] hitCounts = new int[HIT_VALUES];
        // Create the output headers.
        writer.println("contig_id\tlen\tcoverage\tdiff\tpercent\thit1\thit2\thit3\thit4\ttype");
        // Loop through the input.
        log.info("Processing input file.");
        for (TabbedLineReader.Line line : inputStream) {
            // Extract the hit counts.
            for (int i = 0; i < HIT_VALUES; i++) {
                String binId = line.get(i * 2 + 1);
                if (StringUtils.isBlank(binId))
                    hitCounts[i] = 0;
                else
                    hitCounts[i] = line.getInt(i * 2 + 2);
            }
            // Compute the best bin.
            String bestBin = "";
            int diff = hitCounts[0] - hitCounts[1];
            if (diff >= this.minDiff)
                bestBin = line.get(1);
            // Analyze the contig ID.
            String contigId = line.get(0);
            Matcher m = CONTIG_ID_PATTERN.matcher(contigId);
            if (m.matches()) {
                String correctId = m.group(1);
                double coverage = Double.valueOf(m.group(2));
                // Determine the result rating.
                String code;
                if (bestBin.isEmpty()) {
                    if (hitCounts[0] == 0)
                        code = "None";
                    else
                        code = "Ambiguous";
                } else if (bestBin.contentEquals(correctId))
                    code = "Good";
                else
                    code = "Bad";
                int length = this.lenMap.getOrDefault(contigId, 0);
                // Compute the percentage.
                double percent = 0.0;
                if (hitCounts[0] > 0)
                    percent = hitCounts[0] * 100.0 / Arrays.stream(hitCounts).sum();
                // Write the output record.
                writer.format("%s\t%d\t%6.2f\t%d\t%6.2f\t%d\t%d\t%d\t%d\t%s%n", contigId, length, coverage,
                        diff, percent, hitCounts[0], hitCounts[1], hitCounts[2], hitCounts[3], code);
            }
        }
    }

}
