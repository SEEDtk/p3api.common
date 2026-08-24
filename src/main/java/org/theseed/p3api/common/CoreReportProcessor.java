package org.theseed.p3api.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BasePipeProcessor;

/**
 * This command reads the output of the core genome match command (genome.download md5Survey) and produces a report on how many genomes had
 * one match, how many had multiple matches, and how many had no matches.  The report is written to standard output. The output from the
 * match command should come in on the standard input.
 * 
 * The command-line options are as follows.
 * 
 * -h	display command-line usage
 * -i   input file containing the output of the core genome match command (if not STDIN)
 * -o   output file for the report (if not STDOUT)
 */
public class CoreReportProcessor extends BasePipeProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CoreReportProcessor.class);
    /** map of domain names to count of matches by genome */
    private Map<String, CountMap<String>> matchCounters;
    /** count of unmatched genomes by domain name */
    private CountMap<String> noMatchCount;
    /** set of domains found */
    private Set<String> domains;
    /** input column index for core genome ID */
    private int coreCol;
    /** input column index for matched genome ID */
    private int matchCol;
    /** input column index for domain name */
    private int domainCol;
    /** this is used for an empty count map */
    private static final CountMap<String> EMPTY_COUNT_MAP = new CountMap<>();

    @Override
    protected void setPipeDefaults() {
    }

    @Override
    protected void validatePipeInput(TabbedLineReader inputStream) throws IOException {
        // We validate the input by verifying the column headers and saving the column indices.
        this.coreCol = inputStream.findField("genome_id");
        this.matchCol = inputStream.findField("bvbrc_genome_id");
        this.domainCol = inputStream.findField("domain");
    }

    @Override
    protected void validatePipeParms() throws IOException, ParseFailureException {
    }

    @Override
    protected void runPipeline(TabbedLineReader inputStream, PrintWriter writer) throws Exception {
        // Write the output header.
        writer.println("domain\tno_match\tsingle_match\tmulti_match\tgenomes");
        // Create the count maps.
        this.matchCounters = new TreeMap<>();
        this.noMatchCount = new CountMap<>();
        this.domains = new TreeSet<>();
        // Loop through the input, counting the matches.
        int genomesIn = 0;
        for (var line : inputStream) {
            genomesIn++;
            String domain = line.get(this.domainCol);
            this.domains.add(domain);
            String match = line.get(this.matchCol);
            if (match.isBlank())
                this.noMatchCount.count(domain);
            else {
                // Here we need to record the match for this genome in the appropriate domain count map.
                CountMap<String> domainMap = this.matchCounters.computeIfAbsent(domain, x -> new CountMap<>());
                // Count the match for this core genome.
                String coreId = line.get(this.coreCol);
                domainMap.count(coreId);
            }
            if (genomesIn % 1000 == 0)
                log.info("{} genomes processed.", genomesIn);
        }
        log.info("{} total genomes processed.", genomesIn);
        // Set up the grand totals.
        int totalNoMatch = 0;
        int totalSingle = 0;
        int totalMulti = 0;
        int totalGenomes = 0;
        // Write the counts to the output.
        for (String domain : this.domains) {
            // For this domain, the number of unmatched genomes is in the no-match count map.
            int noMatch = this.noMatchCount.getCount(domain);
            CountMap<String> domainMap = this.matchCounters.getOrDefault(domain, EMPTY_COUNT_MAP);
            int single = 0;
            int multi = 0;
            for (var counter : domainMap.counts()) {
                if (counter.getCount() == 1)
                    single++;
                else
                    multi++;
            }
            int genomes = noMatch + single + multi;
            writer.printf("%s\t%12d\t%12d\t%12d\t%12d%n", domain, noMatch, single, multi, genomes);
            totalNoMatch += noMatch;
            totalSingle += single;
            totalMulti += multi;
            totalGenomes += genomes;
        }
        // Write the grand totals.
        writer.println();
        writer.printf("TOTAL\t%12d\t%12d\t%12d\t%12d%n", totalNoMatch, totalSingle, totalMulti, totalGenomes);
    }

}
