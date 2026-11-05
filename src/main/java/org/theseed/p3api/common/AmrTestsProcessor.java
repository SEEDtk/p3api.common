package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.utils.BaseMultiReportProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command reads a file containing genus and drug names and outputs genome lists for susceptible and resistant genomes
 * regarding the named drug in the identified genus. These can then be used for AMR repgen tests.
 * 
 * For efficiency reasons, we will use a soft match on the genome name to filter by genus, since the genome name is in the
 * genome_amr table but not the genus ID or name.
 * 
 * The standard input should be a tab-delimited file with columns "genus_id" for the genus ID, "genus" for the genus name,
 * and "drug" for the drug name.
 * 
 * Two output files will be produced for the genus/drug pair. The file names will be the genus name followed by the drug name
 * and the suffix "resist" or "suscept". All files will be tab-delimited with headers and have the file extension "tbl".
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   name of the input file (if not STDIN)
 * -D   name of the output directory for the genome lists (default is "Tests" in the current directory)
 * 
 */
public class AmrTestsProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /** logging facility */
    protected static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AmrTestsProcessor.class);
    /** connection to the BV-BRC database */
    private P3CursorConnection p3;
    /** input file stream */
    private TabbedLineReader inStream;
    /** input genus ID column index */
    private int genusIdColIdx;
    /** input genus name column index */
    private int genusColIdx;
    /** input drug name column index */
    private int drugColIdx;

    // COMMAND-LINE OPTIONS

    /** name of the input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "inFile", usage = "input file name (if not STDIN)")
    private File inFile;

    @Override
    protected File setDefaultOutputDir(File curDir) {
        File retVal = new File(curDir, "Tests");
        return retVal;
    }

    @Override
    protected void setMultiReportDefaults() {
        this.inFile = null; // default to STDIN
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        if (this.inFile != null && ! this.inFile.canRead())
            throw new IOException("Input file " + this.inFile + " is not found or unreadable.");
        // Set up the input stream and column indices.
        if (this.inFile == null) {
            log.info("Input will be taken from STDIN");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("Input will be taken from file {}", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
        this.genusIdColIdx = this.inStream.findField("genus_id");
        this.genusColIdx = this.inStream.findField("genus");
        this.drugColIdx = this.inStream.findField("drug");
        // Connect to the database.
        this.p3 = new P3CursorConnection();
    }

    @Override
    protected void runMultiReports() throws Exception {
        try {
            // Count the input lines read and the genomes found.
            int lineCount = 0;
            int genomeCount = 0;
            // Loop through the input lines.
            for (TabbedLineReader.Line line : this.inStream) {
                lineCount++;
                String genusId = line.get(this.genusIdColIdx);
                String genus = line.get(this.genusColIdx);
                String drug = line.get(this.drugColIdx);
                log.info("Processing genus {} ({}) for drug {}", genus, genusId, drug);
                // Filter on genome name and drug name in the AMR table to get the AMR records for this
                // genus/drug pair.
                List<JsonObject> amrRecords = this.p3.getRecords("genome_amr", P3CursorConnection.MAX_LIMIT, 
                        "genome_id,genome_name,resistant_phenotype",
                        SolrFilter.EQ("genome_name", genus), SolrFilter.EQ("antibiotic", drug));
                log.info("Found {} AMR records for genus {} and drug {}", amrRecords.size(), genus, drug);
                // We will build the genome data in these maps. Both map genome IDs to names.
                Map<String, String> resistData = new TreeMap<>();
                Map<String, String> susceptData = new TreeMap<>();
                for (JsonObject amrRecord : amrRecords) {
                    String phenotype = KeyBuffer.getString(amrRecord, "resistant_phenotype");
                    String genomeId = KeyBuffer.getString(amrRecord, "genome_id");
                    String genomeName = KeyBuffer.getString(amrRecord, "genome_name");
                    if (phenotype.equals("Resistant"))
                        resistData.put(genomeId, genomeName);
                    else if (phenotype.equals("Susceptible"))
                        susceptData.put(genomeId, genomeName);
                }
                log.info("Found {} resistant and {} susceptible genomes for genus {} and drug {}", resistData.size(), susceptData.size(), genus, drug);
                // Now create the output files and output the genome lists.
                log.info("Writing resistant genomes for genus {} and drug {}", genus, drug);
                try (PrintWriter resistOut = this.openReport(genus + "." + drug + ".resist.tbl")) {
                    this.writeGenomes(resistOut, resistData);
                }
                log.info("Writing susceptible genomes for genus {} and drug {}", genus, drug);
                try (PrintWriter susceptOut = this.openReport(genus + "." + drug + ".suscept.tbl")) {
                    this.writeGenomes(susceptOut, susceptData);
                }
                genomeCount += resistData.size() + susceptData.size();
                log.info("Total genomes output so far: {}", genomeCount);
            }
            log.info("{} genomes output for {} genus/drug combinations", genomeCount, lineCount);
        } finally {
            if (this.inStream != null)
                this.inStream.close();
        }
    }

    /**
     * Write the genome data to the output file. Each line will contain a genome ID and genome name.
     * 
     * @param writer        output print writer
     * @param genomes       map of genome IDs to genome names
     */
    private void writeGenomes(PrintWriter writer, Map<String,String> genomes) {
        writer.println("genome_id\tgenome_name");
        for (Map.Entry<String, String> entry : genomes.entrySet())
            writer.println(entry.getKey() + "\t" + entry.getValue());
    }

}
