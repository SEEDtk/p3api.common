package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.proteins.RoleMap;
import org.theseed.sequence.ProteinKmers;
import org.theseed.sequence.seeds.FinderKmerBatch;
import org.theseed.sequence.seeds.FinderKmerBatch.ProgressTracker;
import org.theseed.sequence.seeds.FinderKmerStats;
import org.theseed.sequence.seeds.FinderKmers;
import org.theseed.utils.BasePipeProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This subcommand generates a representative set of genomes using finder-kmer technology. This is a two-pass process. First,
 * we run through the genomes to get key metrics about the set. We estimate the closeness limit to use based on those
 * metrics. Then we make another pass to generate the representative set itself. A temporary file containing the protein
 * sequences discovered in the first pass will be used to speed up the second pass.
 * 
 * The genome list should be provided on the standard input as a tab-delimited file with headers. The default column for the
 * genome ID will be the first one. The representative-genome list will be written to the standard output.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   input file containing the genome list (if not STDIN)
 * -o   output file for the representative-genome list (if not STDOUT)
 * -c   index (1-based) or name of input column containing the genome IDs
 * -N   target size for the output repgen set (default 200)
 * -b   batch size for processing genome IDs (default 400)
 * 
 * --keep   if TRUE, the temporary file will not be deleted on exit (default is FALSE)
 * --roles  role definition file for the roles to use (default roles.for.finder in the current directory)
 * 
 * --fuzz   a distance from the computed closeness limit, relative to the standard deviation, for alternate limits to try (default is 0.3)
 */
public class FinderRepsProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderRepsProcessor.class);
    /** genome ID list */
    private Set<String> genomeIds;
    /** temporary file for storing protein sequences from the first pass */
    private File tempFileName;
    /** role map for roles to use in genome comparisons */
    private RoleMap roleMap;
    /** ordered list of role IDs for the temporary file */
    private List<String> roleIds;
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;
    /** closeness limit for initial sample (see NOTE) */
    public static final double SAMPLE_LIMIT = 0.8;
    /** array for the above */
    private static final double[] SAMPLE_LIMIT_ARRAY = new double[] { SAMPLE_LIMIT };
    /** Z80 of the sample limit (see NOTE) */
    private static final double Z80 = 0.8416212335729143;
    /** batch size for the second pass */
    private static final int SMALL_BATCH_SIZE = 30;

    /** NOTE the Z80 value is the z-score corresponding to the 80th percentile of the standard normal distribution.
     * If the SAMPLE_LIMIT changes, the Z80 value can be computed as 
     * 
     *   import org.apache.commons.math3.distribution.NormalDistribution;
     *
     *   NormalDistribution standardNormal = new NormalDistribution();
     *   double Z80 = standardNormal.inverseCumulativeProbability(0.8); 
     */


    // COMMAND-LINE OPTIONS

    /** index (1-based) or name of input column containing the genome IDs */
    @Option(name = "--col", aliases = { "-c" }, metaVar = "genome_id", usage = "index (1-based) or name of input column containing the genome IDs")
    private String genomeIdCol;

    /** distance from the computed closeness limit, relative to the standard deviation, for alternate limits to try */
    @Option(name = "--fuzz", metaVar = "0.2", usage = "distance from the computed closeness limit, relative to the standard deviation, for alternate limits to try")
    private double fuzz;

    /** role definition file for the roles to use */
    @Option(name = "--roles", metaVar = "roles.for.finder", usage = "role definition file for the roles to use")
    private File rolesFile;

    /** batch size for processing genome IDs */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "300", usage = "batch size for processing genome IDs")
    private int batchSize;

    /** target number of genomes for the output representative-genome set */
    @Option(name = "--target", aliases = { "-N" }, metaVar = "1000", usage = "target number of genomes for the output representative-genome set")
    private int targetSize;
    
    /** if TRUE, the temporary file will not be deleted on exit */
    @Option(name = "--keep",  usage = "if specified, the temporary file will not be deleted on exit")
    private boolean keepTempFile; 


    @Override
    protected void setPipeDefaults() {
        this.fuzz = 0.3;
        this.genomeIdCol = "1";
        this.keepTempFile = false;
        this.rolesFile = new File("roles.for.finder");
        this.targetSize = 200;
        this.batchSize = 400;
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // Read the genome IDs from the input stream.
        int colIdx = inputStream.findField(this.genomeIdCol);
        log.info("Reading genome IDs from column {}.", colIdx + 1);
        this.genomeIds = new HashSet<>();
        inputStream.stream().forEach(line -> this.genomeIds.add(line.get(colIdx)));
        log.info("{} genome IDs found.", this.genomeIds.size());
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        // Validate the target size.
        if (this.targetSize <= 0)
            throw new ParseFailureException("Target size must be positive.");
        // Validate the fuzz factor.
        if (this.fuzz <= 0.0)
            throw new ParseFailureException("Fuzz factor must be positive.");
        // Load the role map from the specified file.
        if (! this.rolesFile.canRead())
            throw new ParseFailureException("Role definition file " + this.rolesFile + " is not found or unreadable.");
        log.info("Loading role map from {}.", this.rolesFile.getAbsolutePath());
        this.roleMap = RoleMap.load(this.rolesFile);
        log.info("Role map loaded with {} roles.", this.roleMap.size());
        // Save the role IDs in a list to insure we always have the correct order when reading back the temporary file.
        this.roleIds = new ArrayList<>(this.roleMap.keySet());
        // Connect to the database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Check for the edge cases.
        if (this.genomeIds.isEmpty()) {
            log.warn("No genomes to process.");
        } else if (this.genomeIds.size() <= this.targetSize) {
            log.warn("Number of genomes ({}) is less than or equal to the target size ({}).", this.genomeIds.size(), this.targetSize);
            this.outputGenomes(writer, this.genomeIds);
        } else if (this.targetSize == 1) {
            log.warn("Target size is 1, only one representative genome will be selected.");
            Iterator<String> iter = this.genomeIds.iterator();
            if (iter.hasNext())
                this.outputGenomes(writer, Collections.singleton(iter.next()));
        }
        // For the first pass, we will loop through the genome IDs, building and processing
        // batches. Each batch will be used to update a statistics object for the closeness values
        // and create a repgen set for the sample closeness limit. The size of that repgen set
        // will be one of the key metrics for determining the final closeness limit. Finally, we
        // save the ID, name, and protein sequences for each genome in a temporary file so we can
        // make the second pass without using the database.
        log.info("Preparing for first pass through {} genomes.", this.genomeIds.size());
        try {
            // Create the temporary cache file.
            this.tempFileName = File.createTempFile("finder_reps_", ".tmp");
            log.info("Temporary file created at {}.", this.tempFileName.getAbsolutePath());
            // Create a progress tracker so we can monitor the processing of genome batches.
            ProgressTracker progress = new ProgressTracker();
            // Create a statistics object to track the closeness values and attach it to the progress tracker.
            FinderKmerStats stats = new FinderKmerStats();
            progress.setStats(stats);
            // This will be used to hold the size of the sample repgen set.
            int sampleSize;
            // Open the temporary file for output.
            try (PrintWriter tempWriter = new PrintWriter(this.tempFileName)) {
                // Create a FinderKmerBatch to hold the sample repgen set.
                FinderKmerBatch repgen = new FinderKmerBatch();
                // Put it into an array because of the way the processBatch method expects it.
                FinderKmerBatch[] repgenArray = new FinderKmerBatch[] { repgen };
                // Write the header for the temporary file.
                tempWriter.println("genome_id\t" + StringUtils.join(this.roleIds, "\t"));
                // We will loop through the genome IDs, filling the sample batch. Once its full,
                // we generate the repgen set, update the statistics, and cache the proteins in
                // the temporary file.
                Set<String> batch = new HashSet<>();
                for (String genomeId : this.genomeIds) {
                    batch.add(genomeId);
                    if (batch.size() >= this.batchSize) {
                        this.processBatch(batch, tempWriter, repgenArray, progress);
                        batch.clear();
                    }
                }
                // Process any remaining genomes in the batch.
                if (! batch.isEmpty()) {
                    this.processBatch(batch, tempWriter, repgenArray, progress);
                    batch.clear();
                }
                // Record the size of the sample repgen set.
                sampleSize = repgen.size();
            }
            double desiredCloseness = estimateY(stats, this.genomeIds.size(), sampleSize, (double) this.targetSize);
            log.info("Estimated desired closeness: {}", desiredCloseness);
            // We are going to make three repgen sets with three closeness limits. First, we build the array of closeness limits.
            double fuzzValue = stats.getStdDev() * this.fuzz;
            double[] closenessLimits = this.computeClosenessLimits(desiredCloseness, fuzzValue);
            FinderKmerBatch[] repgenArray = this.buildRepgenArray(closenessLimits);
            // Reset the progress tracker for the second pass.
            progress.reset();
            log.info("Computing repgen sets for the second pass.");
            // Prepare to read back the temporary file.
            try (TabbedLineReader tempReader = new TabbedLineReader(this.tempFileName)) {
                // This object will hold the current batch of genomes.
                FinderKmerBatch batch = new FinderKmerBatch();
                // Count the number of lines read.
                int lineCount = 0;
                for (var line : tempReader) {
                    lineCount++;
                    if (batch.size() >= SMALL_BATCH_SIZE) {
                        batch.updateRepgens(repgenArray, closenessLimits, progress);
                        batch.clear();
                    }
                    // Here we know there is room in the batch, so we add this genome.
                    FinderKmers genome = new FinderKmers(line.get(0));
                    // We need to add all the proteins.
                    for (int i = 0; i < roleIds.size(); i++) {
                        String fullProteinString = line.get(i + 1);
                        if (! StringUtils.isBlank(fullProteinString)) {
                            // Split the protein strings for this role. Most of the time, there is exactly one.
                            String[] proteinStrings = StringUtils.split(line.get(i + 1), ',');
                            String roleId = this.roleIds.get(i);
                            for (String proteinString : proteinStrings)
                                genome.addProteinSequence(roleId, proteinString);
                        }
                    }
                    // Add the genome to the current batch.
                    batch.addFinderKmers(genome);
                }
                // Process any remaining genomes in the batch.
                if (batch.size() > 0) {
                    batch.updateRepgens(repgenArray, closenessLimits, progress);
                }
                log.info("Completed second pass with {} lines processed.", lineCount);
            }
            FinderKmerBatch repgen = this.selectBestRepgen(repgenArray);
            log.info("Selected best repgen set with {} genomes.", repgen.size());
            // Output the representative genomes.
            this.outputGenomes(writer, repgen.genomeIds());
        } finally {
            if (this.tempFileName != null && ! this.keepTempFile) {
                log.info("Deleting temporary file at {}.", this.tempFileName.getAbsolutePath());
                FileUtils.forceDelete(this.tempFileName);
                this.tempFileName = null;
            }
        }
    }

    /**
     * Process a batch of genomes during the first pass. We load the genomes from the database, update the statistics
     * merge them into the sample repgen set, and cache the proteins in the temporary file.
     *  
     * @param batch         the set of genome IDs to process in this batch
     * @param tempWriter    the writer for the temporary file to cache proteins
     * @param repgenSample  the sample representative genome set to merge into
     * @param progress      the progress tracker for updating statistics
     * 
     * @throws IOException 
     */
    private void processBatch(Set<String> batch, PrintWriter tempWriter, FinderKmerBatch[] repgenSample,
            ProgressTracker progress) throws IOException {
        log.info("Loading batch of {} genomes for first pass.", batch.size());
        FinderKmerBatch kmerBatch = new FinderKmerBatch();
        kmerBatch.loadBatch(roleMap, p3, batch);
        // Write the cached proteins to the temporary file.
        for (FinderKmers finderKmers : kmerBatch) {
            // Write the genome ID and all its protein sequences to the temporary file.
            String genomeId = finderKmers.getGenomeId();
            tempWriter.print(genomeId);
            for (String roleId : this.roleIds) {
                ProteinKmers proteinKmers = finderKmers.getRoleKmers(roleId);
                String fullProteinString;
                if (proteinKmers == null)
                    fullProteinString = "";
                else
                    fullProteinString = proteinKmers.getFullProteinString();
                tempWriter.print("\t" + fullProteinString);
            }
            tempWriter.println();
        }
        // Merge the batch into the sample representative genome set.
        kmerBatch.updateRepgens(repgenSample, SAMPLE_LIMIT_ARRAY, progress);
    }


    /**
     * Compute the estimated closeness limit (y) for a sample with the specified statistical characteristics, given
     * a desired output repgen size (x)
     * 
     * @param stats         the statistical characteristics of the sample
     * @param size          the number of genomes in the sample
     * @param sample        repgen size at closeness 0.8
     * @param x             desired output repgen size
     * 
     * @return              the estimated closeness limit (y) for the given sample and desired output repgen size
     */
    private static double estimateY(FinderKmerStats stats, int size, double sample, double x) {
        if (sample <= 0 || size <= 0) {
            throw new IllegalArgumentException("size and sample must be positive");
        }
        if (x == sample) {
            return 0.8;
        }

        double logSigma = -1.7128
                - 1.0084 * stats.getMean()
                + 1.6362 * stats.getStdDev()
                + 0.4290 * Math.log((double) size)
                - 0.3801 * Math.log(sample);
        double sigma = Math.exp(logSigma);

        double ratio = Math.max(x, 0.5) / sample;
        double z = Math.log(ratio) / sigma + Z80;
        return normalCdf(z);
    }

    /** Standard normal CDF, Phi(z). */
    private static double normalCdf(double z) {
        return 0.5 * (1.0 + erf(z / Math.sqrt(2.0)));
    }

    /** Abramowitz and Stegun approximation 7.1.26, max error ~1.5e-7. */
    private static double erf(double x) {
        double sign = Math.signum(x);
        double ax = Math.abs(x);
        double t = 1.0 / (1.0 + 0.3275911 * ax);
        double y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t
                - 0.284496736) * t + 0.254829592) * t * Math.exp(-ax * ax);
        return sign * y;
    }    

    /**
     * Compute the closeness limits to use for selecting representative genomes. The base value is
     * the desired closeness. We will also attempt to use the desired closeness plus or minus the fuzz value.
     * If a computed closeness is outside the acceptable range (0.0 to 1.0), it will be ignored.
     * 
     * @param desiredCloseness    the base closeness value to use
     * @param fuzzValue           the amount to vary the closeness above and below the base value
     * @return                    an array of closeness limits to try, including the base value and the fuzzed values
     */
    private double[] computeClosenessLimits(double desiredCloseness, double fuzzValue) {
        List<Double> limits = new ArrayList<>();
        double base = desiredCloseness;
        double lower = base - fuzzValue;
        double upper = base + fuzzValue;
        if (lower >= 0.0) {
            limits.add(lower);
        }
        if (base >= 0.0 && base <= 1.0) {
            limits.add(base);
        }
        if (upper <= 1.0) {
            limits.add(upper);
        }
        log.info("Computed closeness limits: {}", StringUtils.join(limits, ", "));
        return limits.stream().mapToDouble(Double::doubleValue).toArray();
    }

    /**
     * Build an array of empty FinderKmerBatch objects, one per closeness limit provided,
     * to hold the repgen sets.
     * 
     * @param closenessLimits    the array of closeness limits for which to create empty FinderKmerBatch objects
     * @return                    an array of empty FinderKmerBatch objects, one per closeness limit
     */
    private FinderKmerBatch[] buildRepgenArray(double[] closenessLimits) {
        FinderKmerBatch[] retVal = new FinderKmerBatch[closenessLimits.length];
        for (int i = 0; i < closenessLimits.length; i++) {
            retVal[i] = new FinderKmerBatch();
        }
        return retVal;
    }

    /**
     * Find the repgen set whose size is closest to the desired target size.
     * 
     * @param repgenArray    the array of FinderKmerBatch objects to evaluate
     * 
     * @return               the FinderKmerBatch object whose size is closest to the desired target size
     */
    private FinderKmerBatch selectBestRepgen(FinderKmerBatch[] repgenArray) {
        FinderKmerBatch retVal = null;
        int error = Integer.MAX_VALUE;
        for (FinderKmerBatch batch : repgenArray) {
            int currentError = Math.abs(batch.size() - this.targetSize);
            if (currentError < error) {
                error = currentError;
                retVal = batch;
            }
        }
        return retVal;
    }

    /**
     * Write the representative genomes to the output writer. This involves looking up the genome names
     * from the database.
     * 
     * @param writer        the output stream for the representative genome information
     * @param repIds        the collection of representative genome IDs to output
     * 
     * @throws IOException 
     */
    private void outputGenomes(PrintWriter writer, Collection<String> repIds) throws IOException {
        log.info("Retrieving genome names.");
        // Get the genome names.
        List<JsonObject> genomes = this.p3.getRecords("genome", P3CursorConnection.MAX_LIMIT, this.batchSize, "genome_id",
                repIds, "genome_id,genome_name");
        log.info("Writing {} genomes to output.", genomes.size());
        writer.println("genome_id\tgenome_name");
        for (JsonObject genome : genomes) {
            String genomeId = KeyBuffer.getString(genome, "genome_id");
            String genomeName = KeyBuffer.getString(genome, "genome_name");
            writer.println(genomeId + "\t" + genomeName);
        }
        writer.flush();
    }

}
