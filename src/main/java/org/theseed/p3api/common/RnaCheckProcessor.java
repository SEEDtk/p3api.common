/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.genome.iterator.GenomeSource;
import org.theseed.reports.RnaCheckReporter;
import org.theseed.sequence.BatchStreamIterator;
import org.theseed.sequence.DnaDataStream;
import org.theseed.sequence.blast.BlastHit;
import org.theseed.sequence.blast.BlastParms;
import org.theseed.sequence.blast.DnaBlastDB;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command checks the SSU rRNA sequences in the genomes from an input source.  First, the annotated 16s features are
 * located in the genome.  Next, the genome is BLASTed against the SILVA database to find RNA locations.  The results are
 * listed for comparison.
 *
 * The positional parameters are the name of the SILVA database and the name of the input genome source.  The command-line
 * options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	name of the file for the output report (if not STDOUT)
 * -t	type of genome source (PATRIC ID file, directory, master directory)
 *
 * --minS		minimum percent BLAST match for the SILVA sequences (default 95)
 * --maxE		maximum permissible E-value (default 1e-10)
 * --format		format of output report (default LIST)
 *
 * @author Bruce Parrello
 *
 */
public class RnaCheckProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaCheckProcessor.class);
    /** input genome source */
    private GenomeSource genomes;
    /** blast database */
    private DnaBlastDB silvaDB;
    /** report writer */
    private RnaCheckReporter reporter;
    /** BLAST parameters */
    private BlastParms parms;
    /** BLAST batch size */
    private static final int BATCH_SIZE = 20;

    // COMMAND-LINE OPTIONS

    /** minimum percent subject coverage for a legitimate hit */
    @Option(name = "--minSubject", aliases = { "--minS" }, metaVar = "50",
            usage  = "minimum percent of subject sequence that must be hit")
    private double minPctSubject;

    /** maximum E-value */
    @Option(name = "--maxE", aliases = { "--evalue" }, metaVar = "1e-5",
            usage = "maximum permissible e-value for a query")
    private double eValue;

    /** input genome source type */
    @Option(name = "--type", aliases = { "-t" }, usage = "type of input genome source")
    private GenomeSource.Type sourceType;

    /** output report format */
    @Option(name = "--format", usage = "output report format")
    private RnaCheckReporter.Type outFormat;

    /** SILVA RNA FASTA file / BLAST database */
    @Argument(index = 0, metaVar = "silva.fasta", usage = "SILVA NR99 SSU reference RNA FASTA file")
    private File silvaFile;

    /** input genome source */
    @Argument(index = 1, metaVar = "genomeDir", usage = "input genome source (directory or ID file)")
    private File genomeDir;

    @Override
    protected void setReporterDefaults() {
        this.minPctSubject = 95.0;
        this.eValue = 1e-10;
        this.sourceType = GenomeSource.Type.DIR;
        this.outFormat = RnaCheckReporter.Type.LIST;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Verify the BLAST parameters.
        if (this.minPctSubject < 0.0 || this.minPctSubject > 100.0)
            throw new ParseFailureException("Minimum subject percent must be between 0 and 100.");
        if (this.eValue < 0.0)
            throw new ParseFailureException("Maximum eValue cannot be negative.");
        // Insure we have a silva database.
        if (! this.silvaFile.canRead())
            throw new FileNotFoundException("Silva FASTA file " + this.silvaFile + " is not found or unreadable.");
        // Insure we have a genome source.
        if (! this.genomeDir.exists())
            throw new FileNotFoundException("Input genome source " + this.genomeDir + " does not exist.");
        // Build or load the BLAST database.  Note we have to convert the possible InterruptedException.
        try {
            log.info("Connecting to BLAST database at {}.", this.silvaFile);
            this.silvaDB = DnaBlastDB.createOrLoad(this.silvaFile, 11);
        } catch (InterruptedException e) {
            throw new IOException("Interruption during BLAST database make: " + e.toString());
        }
        // Connect to the genome source.
        log.info("Loading genomes at {}.", this.genomeDir);
        this.genomes = this.sourceType.create(this.genomeDir);
        log.info("{} genomes found in {}.", this.genomes.size(), this.genomeDir);
        // Create the blast parameters.
        this.parms = new BlastParms().maxE(this.eValue).pctLenOfSubject(this.minPctSubject);
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Create and initialize the report.
        this.reporter = this.outFormat.create(writer);
        this.reporter.openReport();
        // Loop through the genomes.  We count the number of genomes processed, the number of annotated RNAs found,
        // and the number of SILVA RNAs found.
        int gCount = 0;
        int annoRna = 0;
        int blastRna = 0;
        for (Genome genome : this.genomes) {
            gCount++;
            log.info("Processing genome {} of {}: {}.", gCount, this.genomes.size(), genome);
            this.reporter.openGenome(genome);
            // Get the annotated SSU rRNAs.
            annoRna += this.searchForAnnotatedRna(genome);
            // Get the BLAST SSU rRNAs.
            blastRna += this.blastForRna(genome);
            this.reporter.closeGenome(genome);
        }
        // All done.  Finish the report.
        this.reporter.finish();
        log.info("{} genomes processed.  {} RNAs found by BLAST, {} from annotations.", gCount, blastRna, annoRna);
    }

    /**
     * BLAST against the Silva database to find SSU rRNA in the specified genome.  The RNAs found will be
     * sent to the report writer.
     *
     * @param genome	genome of interest
     *
     * @return the number of RNAs found
     */
    private int blastForRna(Genome genome) {
        // Collect the contigs from the genome.
        DnaDataStream contigs = new DnaDataStream(genome);
        // This will hold the locations found.  Each location will be mapped to the associated hit's description (a taxonomy
        // string).  If two locations overlap on the same strand, they will be merged, but only the first hit's description is
        // kept.
        List<RnaDescriptor> descriptors = new ArrayList<RnaDescriptor>();
        // BLAST the contigs against the Silva database in batches.
        DnaDataStream buffer = new DnaDataStream(BATCH_SIZE, 11);
        BatchStreamIterator iter = new BatchStreamIterator(contigs, buffer, BATCH_SIZE);
        while (iter.hasNext()) {
            DnaDataStream newContigs = (DnaDataStream) iter.next();
            List<BlastHit> hits = this.silvaDB.blast(newContigs, this.parms);
            log.info("{} hits found against {}.", hits.size(), genome);
            for (BlastHit hit : hits) {
                // Create a descriptor for the hit.
                RnaDescriptor thisHit = new RnaDescriptor(genome, hit);
                // Check to see if it merges with an existing hit.
                boolean merged = false;
                for (int i = 0; i < descriptors.size() && ! merged; i++)
                    merged = descriptors.get(i).checkForMerge(thisHit);
                if (! merged)
                    descriptors.add(thisHit);
            }
        }
        // Record the BLAST hits from this genome.
        for (RnaDescriptor descriptor : descriptors)
            this.reporter.recordHit(descriptor);
        // Return the number found.
        return descriptors.size();
    }

    /**
     * Search the RNA features in a genome to find SSU rRNAs.
     *
     * @param genome	genome to search
     *
     * @return the number of 16s RNA features found
     */
    private int searchForAnnotatedRna(Genome genome) {
        int retVal = 0;
        for (Feature feat : genome.getFeatures()) {
            if (feat.getType().contentEquals("rna") && Genome.SSU_R_RNA.matcher(feat.getPegFunction()).find()) {
                RnaDescriptor descriptor = new RnaDescriptor(genome, feat);
                this.reporter.recordHit(descriptor);
                retVal++;
            }
        }
        return retVal;
    }

}
