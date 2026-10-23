package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * This command compares the good-genome list with the genomes in a finder-kmer load file. It also tracks the number
 * of missing proteins for each role ID.
 * 
 * This is not a very smart program because it deals with a temporary emergency situation.
 * 
 * The positional parameter is the name of the finder-kmer load file. The standard input should contain the
 * good-genomes file. The list of genomes not found will be written to the standard output, and the role ID
 * stats will be written to the log.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   name of the good-genomes file (if not STDIN)
 * -o   name of the output file for genomes not found (if not STDOUT)
 * 
 */
public class FinderFixProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderFixProcessor.class);
    /** set of genomes from the good-genomes file, mapped from ID to name */
    private Map<String, String> goodGenomes;
    /** missing-proteins counter for each role ID */
    private CountMap<String> missingCounts;
    /** index of the genome ID column in the good-genomes file */
    private int genomeIdColIdx;
    /** index of the genome name column in the good-genomes file */
    private int genomeNameColIdx;

    // COMMAND-LINE OPTIONS

    /** name of the protein finder directory */
    @Argument(index = 0, metaVar = "loadFile", usage = "name of the finder-kmer load file to check", required = true)
    private File loadFile;


    @Override
    protected void setPipeDefaults() {
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
        if (! this.loadFile.canRead())
            throw new IOException("Finder-kmer load file " + this.loadFile + " is not found or unreadable.");
    }

    @Override
    protected void validatePipeInput(TabbedLineReader reader) throws IOException {
        // Insure we have the two columns we need.
        this.genomeIdColIdx = reader.findField("genome_id");
        this.genomeNameColIdx = reader.findField("name");
    }

    @Override
    protected void runPipeline(TabbedLineReader reader, PrintWriter writer) throws Exception {
        // Form the genome IDs into the goodGenomes set.
        this.goodGenomes = new HashMap<>();
        while (reader.hasNext()) {
            var line = reader.next();
            this.goodGenomes.put(line.get(this.genomeIdColIdx), line.get(this.genomeNameColIdx));
        }
        log.info("Finished loading good genome IDs. {} genomes found.", this.goodGenomes.size());
        // Now we process the 
        // We read through the key protein FASTA file, removing the genomes found from the good-genomes set.
        int genomesIn = 0;
        int warnCount = 0;
        this.missingCounts = new CountMap<>();
        try (TabbedLineReader loadStream = new TabbedLineReader(this.loadFile)) {
            log.info("Reading finder-kmer load file {}.", this.loadFile);
            // Get the role IDs. The roles start in the second column (index 1) of the load file.
            String[] labels = loadStream.getLabels();
            while (loadStream.hasNext()) {
                var line = loadStream.next();
                genomesIn++;
                String genomeId = line.get(0);
                // Check for missing roles.
                for (int i = 1; i < labels.length; i++) {
                    if (StringUtils.isBlank(line.get(i)))
                        this.missingCounts.count(labels[i]);
                }
                // Check for a duplicate genome ID.
                if (! this.goodGenomes.containsKey(genomeId)) {
                    log.warn("Duplicate genome ID {} found in load file.", genomeId);
                    warnCount++;
                }
                // Denote we've seen this genome and it's not missing.
                this.goodGenomes.remove(genomeId);
            }
        }
        log.info("Finished processing finder-kmer load file. {} genomes read. {} genomes remain in good-genomes set.", 
                genomesIn, this.goodGenomes.size());
        log.info("{} duplicate genome IDs found in load file.", warnCount);
        if (! this.goodGenomes.isEmpty()) {
            writer.println("genome_id\tgenome_name");
            for (var genomeEntry : this.goodGenomes.entrySet()) {
                String genomeId = genomeEntry.getKey();
                String genomeName = genomeEntry.getValue();
                writer.println(genomeId + "\t" + genomeName);
            }
        }
        log.info("Finished writing missing genomes to output.");
        for (var roleEntry : this.missingCounts.counts()) {
            String role = roleEntry.getKey();
            int count = roleEntry.getCount();
            log.info("Role {} is missing in {} genomes.", role, count);
        }
        log.info("{} total missing roles.", this.missingCounts.sum());
    }

}
