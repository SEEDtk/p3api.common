/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.io.LineReader;
import org.theseed.io.TabbedLineReader;
import org.theseed.proteins.kmers.reps.RepGenome;
import org.theseed.proteins.kmers.reps.RepGenomeDb;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.Sequence;
import org.theseed.utils.BaseReportProcessor;

/**
 * This command determines the distances involved in hammer-bin matches that are not correct.
 * The standard input should be a result file from the fake-fasta hammer test.  This is a tab-delimited
 * file with headers.  The first column is a contig ID, in which the correct representative genome is
 * encoded between the first and second underscores.  The second column is the representative genome
 * actually found; the third is the number of hits.
 *
 * The positional parameters are the name of the directory containing the FASTA files used to run the
 * test, and the name of the P3Eval directory for the representative genomes used.
 *
 * In the P3Eval directory, we will use the scatter.tbl file to build a map from the target representative
 * genomes to the genomes actually used, which are found in the Scatter subdirectory.  These can then be
 * used to determine distances and similarity scores between the genomes used and the representative
 * genomes in question, which we will compute using the rep200.ser file.
 *
 * A report will be produced on the standard output containing the number of hits and the distance
 * information.
 *
 * The number of hits is influenced both by the distance and the contig length, so the first task is to
 * read through the FASTA files to compute the length and file location of each contig.  Next, we will
 * read through the input file to get the relevant genome pair and hit count for each contig.  Finally,
 * we will read the distance file and output the distances and kmer similarities along with the other information.
 *
 * The command-line options are:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing matches (if not STDIN)
 * -o	output file for report (if not STDOUT)
 * -m	minimum number of hits for a hit to be considered good
 *
 * @author Bruce Parrello
 *
 */
public class HammerCheckProcessor extends BaseReportProcessor {

    /**
     * This object tracks the information we need on each contig.
     */
    private static class ContigInfo {

        /** contig ID */
        private String contigId;
        /** source file name */
        private String sourceFile;
        /** contig length */
        private int len;
        /** search pattern for extracting target genome ID */
        private static Pattern CONTIG_ID_PATTERN = Pattern.compile("REP_(\\d+\\.\\d+)_.+");

        /**
         * Create a contig descriptor.
         *
         * @param file		name of source file
         * @param seq		sequence record for the contig
         */
        public ContigInfo(File file, Sequence seq) {
            this.sourceFile = file.getName();
            this.contigId = seq.getLabel();
            this.len = seq.getSequence().length();
        }

        /**
         * @return the contig ID
         */
        public String getContigId() {
            return this.contigId;
        }

        /**
         * @return the source file name
         */
        public String getSourceFile() {
            return this.sourceFile;
        }

        /**
         * @return the contig length
         */
        public int getLen() {
            return this.len;
        }

        /**
         * @return the target representative genome ID for this contig
         *
         * @throws IOException
         */
        public String getTarget() throws IOException {
            Matcher m = CONTIG_ID_PATTERN.matcher(this.contigId);
            if (! m.matches())
                throw new IOException("Invalid contig ID \"" + this.contigId + "\" in file " + this.sourceFile + ".");
            return m.group(1);
        }

    }

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerCheckProcessor.class);
    /** map of contig IDs to descriptors */
    private Map<String, ContigInfo> contigMap;
    /** input file stream */
    private LineReader inStream;
    /** FASTA files in the FASTA directory */
    private Collection<File> fastaFiles;
    /** representative-genome database */
    private RepGenomeDb rep200db;
    /** map of target genome IDs to source genomes */
    private Map<String, Genome> sourceMap;
    /** default representation object */
    private RepGenomeDb.Representation NO_REP;
    /** array of header fields */
    private static final String[] HEADERS = new String[] {
            "source_file", "contig_id", "contig_len", "contig_genome",
            "hits", "correct", "hit_type",
            "target", "target_sim", "target_dist",
            "actual", "actual_sim", "actual_dist",
            "contig_genome_name" };
    /** array of formats */
    private static final String[] FORMATS = new String[] {
            "%s", "%s", "%d", "%s",
            "%d", "%s", "%s",
            "%s", "%d", "%6.4f",
            "%s", "%d", "%6.4f",
            "%s" };
    /** actual header line */
    private static final String HEADER_LINE = StringUtils.join(HEADERS, '\t');
    /** actual format line */
    private static final String FORMAT_LINE = StringUtils.join(FORMATS, '\t') + "%n";

    // COMMAND-LINE OPTIONS

    /** minimum number of hits for a good hit */
    @Option(name = "--minHits", aliases = { "-m" }, usage = "minimum hit count for a good hit")
    private int minHits;

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, usage = "file containing input binning data (if not STDIN)")
    private File inFile;

    /** name of the FASTA file directory */
    @Argument(index = 0, metaVar = "fastaDir", usage = "directory of FASTA files that were binned", required = true)
    private File fastaDir;

    /** name of the P3Eval directory */
    @Argument(index = 1, metaVar = "P3EvalDir", usage = "P3Eval directory containing the representation data",
            required = true)
    private File evalDir;


    @Override
    protected void setReporterDefaults() {
        this.inFile = null;
        this.minHits = 100;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Validate the min-hit count.
        if (this.minHits <= 0)
            throw new ParseFailureException("Minimum hit count must be positive.");
        // Set up the input.
        if (this.inFile == null) {
            log.info("Hit information will be read from the standard input.");
            this.inStream = new LineReader(System.in);
        } else if (! this.inFile.canRead())
            throw new FileNotFoundException("Input file " + this.inFile + " is not found or unreadable.");
        else {
            log.info("Hit information will be read from {}.", this.inFile);
            this.inStream = new LineReader(this.inFile);
        }
        // Find the FASTA files.
        if (! this.fastaDir.isDirectory())
            throw new FileNotFoundException("FASTA directory " + this.fastaDir + " is not found or invalid.");
        else {
            this.fastaFiles = FileUtils.listFiles(this.fastaDir, new String[] { "fna", "fa" }, false);
            log.info("{} FASTA files found in {}.", this.fastaFiles.size(), this.fastaDir);
        }
        // Verify the P3Eval directory.
        if (! this.evalDir.isDirectory())
            throw new FileNotFoundException("Evaluation directory " + this.evalDir + " is not found or invalid.");
        // Load the repgen database.
        log.info("Loading repgen database.");
        this.rep200db = RepGenomeDb.load(new File(this.evalDir, "rep200.ser"));
        // Create the default representation (symbolizing no representative).
        NO_REP = this.rep200db.new Representation();
        // Load the genome map.
        log.info("Loading genome map.");
        GenomeSource scatterGenomes = GenomeSource.Type.DIR.create(new File(this.evalDir, "Scatter"));
        this.sourceMap = new HashMap<String, Genome>(scatterGenomes.size() * 4 / 3);
        try (TabbedLineReader scatterStream = new TabbedLineReader(new File(this.evalDir, "scatter.tbl"))) {
            int repCol = scatterStream.findField("rep200");
            int gCol = scatterStream.findField("genome_id");
            for (TabbedLineReader.Line line : scatterStream) {
                String genomeId = line.get(gCol);
                String repId = line.get(repCol);
                if (this.sourceMap.containsKey(repId))
                    throw new IOException("Duplicate rep ID " + repId + " in scatter file for " + genomeId + ".");
                Genome genome = scatterGenomes.getGenome(genomeId);
                this.sourceMap.put(repId, genome);
            }
            log.info("{} genomes loaded from scatter mapping.", this.sourceMap.size());
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        try {
            // First, we read the FASTA files to determine the contig lengths.  This builds our contigInfo map.
            // Note we estimate 120 contigs per genome.  (Allocating 50 gives us hash margin.)
            this.contigMap = new HashMap<String, ContigInfo>(this.sourceMap.size() * 150);
            // Loop through the FASTA files.
            for (File fastaFile : this.fastaFiles) {
                log.info("Processing FASTA file {}.", fastaFile);
                try (FastaInputStream fastaStream = new FastaInputStream(fastaFile)) {
                    int seqCount = 0;
                    for (Sequence contig : fastaStream) {
                        // Create the descriptor for this contig.
                        ContigInfo contigInfo = new ContigInfo(fastaFile, contig);
                        this.contigMap.put(contigInfo.getContigId(), contigInfo);
                        seqCount++;
                        if (log.isInfoEnabled() && seqCount % 100 == 0)
                            log.info("{} sequences read from {}.", seqCount, fastaFile);
                    }
                }
            }
            log.info("{} contigs processed.", this.contigMap.size());
            // Now that we have the source file and length for each contig, we can begin processing the
            // input.  For each contig, we will output the source file name, ID, length, hit count,
            // target genome, similarity and distance to the target genome, actual closest genome,
            // and similarity and distance to the actual closest genome.  Our first task is to output
            // the file header.
            writer.println(HEADER_LINE);
            // Now we loop through the input.  Note we use NULL as the section delimiter to read the whole file.
            log.info("Processing input.");
            int lineCount = 0;
            int skipCount = 0;
            for (String[] line : this.inStream.new Section(null)) {
                // Get the contig ID, the hit count, and the actual genome.
                String contigId = line[0];
                String actualGenomeId = "";
                int hitCount = 0;
                String hitType = "none";
                if (line.length > 2) {
                    // Here there were hits, so we have an actual and a hit count.
                    actualGenomeId = line[1];
                    hitCount = Integer.valueOf(line[2]);
                    hitType = (hitCount < this.minHits ? "low" : "GOOD");
                }
                // Get the target representative genome ID.  From this we can determine the source genome.
                ContigInfo contigInfo = this.contigMap.get(contigId);
                String targetGenomeId = contigInfo.getTarget();
                // Only proceed if the target genome is valid.
                if (! this.sourceMap.containsKey(targetGenomeId))
                    skipCount++;
                else {
                    Genome sourceGenome = this.sourceMap.get(targetGenomeId);
                    // Extract the source genome's seed protein.
                    RepGenome sourceSeed = this.rep200db.getSeedProtein(sourceGenome);
                    // Compute the distance and similarity to the actual and the target. There is always a target,
                    // but there may not be an actual.
                    var actual = NO_REP;
                    if (! actualGenomeId.isEmpty()) {
                        RepGenome actualSeed = this.rep200db.get(actualGenomeId);
                        actual = this.rep200db.new Representation(actualSeed, sourceSeed);
                    }
                    RepGenome targetSeed = this.rep200db.get(targetGenomeId);
                    var target = this.rep200db.new Representation(targetSeed, sourceSeed);
                    // Determine whether or not this is a correct hit.
                    String correct = (actualGenomeId.equals(targetGenomeId) ? "Y" : "");
                    // We now have all the information we need.
                    writer.format(FORMAT_LINE,
                            contigInfo.getSourceFile(), contigInfo.getContigId(), contigInfo.getLen(), sourceGenome.getId(),
                            hitCount, correct, hitType,
                            targetGenomeId, target.getSimilarity(), target.getDistance(),
                            actualGenomeId, actual.getSimilarity(), actual.getDistance(),
                            sourceGenome.getName()
                            );
                    lineCount++;
                    if (log.isInfoEnabled() && lineCount % 100 == 0)
                        log.info("{} input lines processed, {} skipped.", lineCount, skipCount);
                }
            }
            log.info("{} input lines completed, {} skipped.", lineCount, skipCount);
        } finally {
            // If we were reading from a file, insure it is closed.
            if (this.inFile != null)
                this.inStream.close();
        }
    }


}
