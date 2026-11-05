package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.stats.QualityCountMap;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This processor writes statistics for AMR data at the genus level. It takes as input the bvbrc.good.tbl file listing all good genomes
 * and their genus IDs. For each genome, it gets the associated AMR records and compiles a table of resistance and susceptibility data
 * by genus for each drug. The genus/drug pairs with sufficiently high resistance and susceptibility sets are then output. These sets
 * can be used to test whats-changed functionality.
 * 
 * This is a two-pass algorithm. The first pass counts the genomes by genus and selects the genera that have enough genomes to be considered.
 * The second pass compiles the resistance and susceptibility data for the selected genera.
 * 
 * The input file should be specified as the positional parameter and the report will be produced to the standard output. The command-line 
 * options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -o   output file for the report (if not STDOUT)
 * 
 * --min    minimum number of genomes in the smallest set for a genus/drug pair to be output
 */
public class AmrGeneraProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AmrGeneraProcessor.class);
    /** map of genus IDs to genus names */
    private Map<Integer, String> genusMap;
    /** susceptible (good) and resistant (bad) counts by genus/drug pair */
    private QualityCountMap<GenusDrugPair> countMap;
    /** index of the genome ID column in the bvbrc.good.tbl file */
    private int genomeColIdx;
    /** index of the genus ID column in the bvbrc.good.tbl file */
    private int genusColIdx;
    /** number of AMR records processed */
    private int amrCount;
    /** connection to the BV-BRC database for retrieving genus names */
    private P3CursorConnection p3;
    /** batch size for database queries */
    private static final int BATCH_SIZE = 200;
    /** hash map size for batch data */
    private static final int HASH_MAP_SIZE = BATCH_SIZE * 3 / 2 + 1;

    // COMMAND-LINE OPTIONS

    /** minimum number of genomes in the smallest set for a genus/drug pair to be output */
    @Option(name = "--min", metaVar = "100", usage = "minimum number of genomes in the smallest set for a genus/drug pair to be output")
    private int minGenomes;

    /** input file for the good-genomes data */
    @Argument(index = 0, metaVar = "bvbrc.good.tbl", usage = "input file for the good-genomes data", required = true)
    private File inputFile;

    /**
     * This record represents a genus/drug pair.
     * 
     * @param genus     genus ID
     * @param drug      antibiotic name
     */
    protected record GenusDrugPair(int genus, String drug) implements Comparable<GenusDrugPair> {
        @Override
        public int compareTo(GenusDrugPair other) {
            int retVal = this.drug.compareTo(other.drug);
            if (retVal == 0) retVal = Integer.compare(this.genus, other.genus);
            return retVal;
        }
    }

    @Override
    protected void setReporterDefaults() {
        this.minGenomes = 200;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Insure the minimum number of genomes is positive.
        if (this.minGenomes <= 0)
            throw new ParseFailureException("Minimum number of genomes must be positive.");
        // Verify the input file exists and is readable.
        if (! this.inputFile.canRead())
            throw new IOException("Input file " + this.inputFile + " is not found or unreadable.");
        // Connect to the BV-BRC database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // The first task is to scan the input file for distinct genera.
        this.findGenera();
        // We now loop through all the genomes. If a genome belongs to a genus of interest,
        // we process its AMR records. The AMR records are processed in batches to reduce the number
        // of database calls.
        try (TabbedLineReader reader = new TabbedLineReader(this.inputFile)) {
            log.info("Searching for useful genera in the input file.");
            // Create the count map.
            this.countMap = new QualityCountMap<>();
            // Count the number of lines read, the number of genomes selected, and the number of batches.
            int lineCount = 0;
            int genomeCount = 0;
            int batchCount = 0;
            // The current batch will be built in here.
            Map<String, Integer> batch = new HashMap<>(HASH_MAP_SIZE);
            // The batch processor will count the number of AMR records processed.
            this.amrCount = 0;
            // Loop through the input file.
            for (var line : reader) {
                lineCount++;
                int genus = line.getInt(this.genusColIdx);
                if (this.genusMap.containsKey(genus)) {
                    genomeCount++;
                    batch.put(line.get(this.genomeColIdx), genus);
                    if (batch.size() >= BATCH_SIZE) {
                        batchCount++;
                        log.info("Processing batch {} with {} genomes.", batchCount, batch.size());
                        this.processBatch(batch);
                        batch.clear();
                    }
                }
            }
            // Process any remaining genomes in the last batch.
            if (! batch.isEmpty()) {
                batchCount++;
                log.info("Processing final batch {} with {} genomes.", batchCount, batch.size());
                this.processBatch(batch);
            }
            log.info("{} genomes found in {}. {} were in the {} useful genera.", lineCount, this.inputFile, genomeCount, this.genusMap.size());
            log.info("{} batches processed with {} AMR records producing {} genus/drug pairs.", batchCount, this.amrCount, this.countMap.size());
        }
        log.info("Generating report.");
        writer.println("genus_id\tgenus_name\tdrug\tresistant\tsusceptible");
        int pairCount = 0;
        for (GenusDrugPair pair : this.countMap.bestKeys()) {
            int genusId = pair.genus();
            String drugName = pair.drug();
            int goodCount = this.countMap.good(pair);
            int badCount = this.countMap.bad(pair);
            if (goodCount >= this.minGenomes && badCount >= this.minGenomes) {
                writer.println(genusId + "\t" + this.genusMap.get(genusId) + "\t" + drugName + "\t" + goodCount + "\t" + badCount);
                pairCount++;
            }
        }
        log.info("{} genus/drug pairs written to the report.", pairCount);
    }

    /**
     * Run through the good-genomes file and count the number of genomes for each genus. If there are at least
     * 2 * minGenomes in a genus, then put the genus in the genus map.
     * 
     * @throws IOException if the input file cannot be read or a database error occurs
     */
    private void findGenera() throws IOException {
        CountMap<String> genusCounts = new CountMap<>();
        log.info("Reading good-genomes file " + this.inputFile + " for genus counts.");
        try (TabbedLineReader reader = new TabbedLineReader(this.inputFile)) {
            this.genomeColIdx = reader.findField("genome_id");
            this.genusColIdx = reader.findField("genus");
            for (TabbedLineReader.Line line : reader) {
                String genus = line.get(this.genusColIdx);
                genusCounts.count(genus);
            }
        }
        log.info("{} distinct genera found.", genusCounts.size());
        // Get a set of genera having at least 2 * minGenomes. Note we have to 
        Set<String> genusIdSet = new HashSet<>(2 * genusCounts.size());
        final int minCount = 2 * this.minGenomes;
        for (var counter : genusCounts.counts()) {
            if (counter.getCount() >= minCount)
                genusIdSet.add(counter.getKey());
        }
        log.info("{} genera meet the minimum count of {}.", genusIdSet.size(), minCount);
        this.genusMap = new HashMap<>(genusIdSet.size() * 3 / 2 + 1);
        // Now we generate an id/name mapping for the selected genera. Note we get the integer version
        // of the genus ID here.
        List<JsonObject> genusList = p3.getRecords("taxon", P3CursorConnection.MAX_LIMIT, BATCH_SIZE, 
                "taxon_id", genusIdSet, "taxon_id_i,taxon_name");
        for (JsonObject genus : genusList) {
            int genusId = KeyBuffer.getInt(genus, "taxon_id_i");
            String genusName = KeyBuffer.getString(genus, "taxon_name");
            this.genusMap.put(genusId, genusName);
        }
        log.info("{} genera successfully mapped to names.", this.genusMap.size());
    }

    /**
     * Process a batch of genomes. We get the AMR records for each genome and update the counts accordingly.
     * Note we may encounter genomes that do not have any AMR records, and we may encounter genomes with
     * multiple AMR records for the same drug. We handle this by creating sets for susceptible and resistant
     * genome IDs for each drug. These are then used to update the overall counts for genus/drug pairs.
     * 
     * @param batch     batch of genomes to process, mapping genome IDs to genus IDs
     * 
     * @throws IOException if a database error occurs while retrieving AMR records
     */
    private void processBatch(Map<String,Integer> batch) throws IOException {
        // Get a map of drug names to genome ID sets for susceptible and resistant genomes.
        Map<String, Set<String>> susceptibleMap = new HashMap<>(HASH_MAP_SIZE);
        Map<String, Set<String>> resistantMap = new HashMap<>(HASH_MAP_SIZE);
        // Now we get the AMR records for the genomes in the batch.
        p3.getRecords("genome_amr", P3CursorConnection.MAX_LIMIT, BATCH_SIZE, "genome_id", batch.keySet(), "antibiotic,genome_id,resistant_phenotype",
                Collections.emptyList(), x -> this.processAmr(x, susceptibleMap, resistantMap));
        // Update counts from the susceptible and resistant maps.
        this.mergeMap(susceptibleMap, batch, x -> this.countMap.setGood(x));
        this.mergeMap(resistantMap, batch, x -> this.countMap.setBad(x));
    }

    /**
     * Merge the genome ID sets from the source map into the overall counts using the provided consumer.
     * Each entry in the source map represents a drug and the set of genome IDs associated with it. The
     * consumer is applied to each genus/drug pair derived from the genome IDs in the set.
     * 
     * @param sourceMap     map of drug names to sets of genome IDs
     * @param genomeBatch    map of genome IDs to genus IDs for the current batch
     * @param consumer      consumer to apply to each genus/drug pair derived from the genome IDs in the source map
     */
    private void mergeMap(Map<String,Set<String>> sourceMap, Map<String,Integer> genomeBatch, Consumer<GenusDrugPair> consumer) {
        for (Map.Entry<String, Set<String>> entry : sourceMap.entrySet()) {
            String drug = entry.getKey();
            for (String genomeId : entry.getValue()) {
                Integer genusId = genomeBatch.get(genomeId);
                if (genusId != null)
                    consumer.accept(new GenusDrugPair(genusId, drug));
            }
        }
    }

    /**
     * Process a single AMR record, updating the susceptible and resistant maps as appropriate.
     * 
     * @param amrRecord         record to process
     * @param susceptibleMap    map of drug names to sets of susceptible genome IDs
     * @param resistantMap      map of drug names to sets of resistant genome IDs
     */
    private void processAmr(JsonObject amrRecord, Map<String,Set<String>> susceptibleMap,
            Map<String,Set<String>> resistantMap) {
        String phenotype = KeyBuffer.getString(amrRecord, "resistant_phenotype");
        String genomeId = KeyBuffer.getString(amrRecord, "genome_id");
        String drug = KeyBuffer.getString(amrRecord, "antibiotic");
        if (phenotype.equalsIgnoreCase("susceptible")) {
            susceptibleMap.computeIfAbsent(drug, k -> new HashSet<>()).add(genomeId);
        } else if (phenotype.equalsIgnoreCase("resistant")) {
            resistantMap.computeIfAbsent(drug, k -> new HashSet<>()).add(genomeId);
        }
        this.amrCount++;
    }

    
}
