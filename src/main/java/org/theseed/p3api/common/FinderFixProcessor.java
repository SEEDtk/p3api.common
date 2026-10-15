package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Feature;
import org.theseed.io.TabbedLineReader;
import org.theseed.sequence.FastaInputStream;
import org.theseed.sequence.Sequence;
import org.theseed.utils.BaseInputProcessor;

/**
 * This command fixes missing genomes in the protein finder FASTA file for the key protein. The key protein should have
 * exactly one record for each good genome. If a good genome is missing, we list it for future fixing.
 * 
 * This is not a very smart program because it deals with a temporary emergency situation.
 * 
 * The positional parameters are the name of the protein finder directory to fix. The standard input should contain the
 * good-genomes file.
 * 
 * The command-line options are as follows:
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -i   name of the good-genomes file (if not STDIN)
 * 
 */
public class FinderFixProcessor extends BaseInputProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FinderFixProcessor.class);
    /** set of genomes from the good-genomes file */
    private Set<String> goodGenomes; 
    /** name of the key protein FASTA file */
    private File keyProteinFasta;
    /** ID of the key role */
    private static final String KEY_ROLE_ID = "PhenTrnaSyntAlph";

    // COMMAND-LINE OPTIONS

    /** name of the protein finder directory */
    @Argument(index = 0, metaVar = "finderDir", usage = "name of the protein finder directory to fix", required = true)
    private File finderDir;


    @Override
    protected void setReaderDefaults() {
    }

    @Override
    protected void validateReaderParms() throws IOException, ParseFailureException {
        if (! this.finderDir.isDirectory())
            throw new IOException("Finder directory " + this.finderDir + " is not found or invalid.");
        // Insure the key protein FASTA exists.
        this.keyProteinFasta = new File(this.finderDir, KEY_ROLE_ID + ".fna");
        if (! this.keyProteinFasta.canRead())
            throw new IOException("Key protein FASTA file " + this.keyProteinFasta + " is not found or unreadable.");
    }

    @Override
    protected void validateReaderInput(TabbedLineReader reader) throws IOException {
    }

    @Override
    protected void runReader(TabbedLineReader reader) throws Exception {
        // Form the genome IDs into the goodGenomes set.
        this.goodGenomes = new HashSet<>();
        while (reader.hasNext()) {
            var line = reader.next();
            this.goodGenomes.add(line.get(0));
        }
        log.info("Finished copying good genome IDs. {} genomes found.", this.goodGenomes.size());
        // We read through the key protein FASTA file, removing the genomes found from the good-genomes set.
        int genomesIn = 0;
        try (FastaInputStream fasta = new FastaInputStream(this.keyProteinFasta)) {
            log.info("Reading protein DNA sequences from the key protein FASTA file {}.", this.keyProteinFasta);
            long lastMsg = System.currentTimeMillis();
            for (Sequence record : fasta) {
                // Denote that we have seen this genome.
                String genomeId = Feature.genomeOf(record.getLabel());
                this.goodGenomes.remove(genomeId);
                genomesIn++;
                long now = System.currentTimeMillis();
                if (now - lastMsg > 5000) {
                    log.info("Processed key protein FASTA for {} genomes so far. {} left in set.", genomesIn, this.goodGenomes.size());
                    lastMsg = now;
                }
            }
            log.info("Finished processing key protein FASTA. {} genomes remain.", this.goodGenomes.size());
            if (! this.goodGenomes.isEmpty()) {
                for (String genomeId : this.goodGenomes) {
                    log.error("Genome {} is missing from the FASTA file.", genomeId);
                }
            }
        }
    }

    


}
