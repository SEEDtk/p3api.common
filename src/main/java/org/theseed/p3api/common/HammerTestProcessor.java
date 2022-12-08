/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.proteins.DnaTranslator;
import org.theseed.proteins.Role;
import org.theseed.proteins.RoleMap;
import org.theseed.sequence.ProteinKmers;
import org.theseed.sequence.RnaKmers;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This command compares feature distances for two genomes.  It outputs the similarity between the protein sequences, the similarity
 * between the DNA sequences, and the similarity between the translated DNA sequence and the protein translation.
 *
 * The positional parameters are the names of the GTO files.
 *
 * Output is to the log.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --roles	the name of the role definition file for the roles to process (default "roles.for.hammers" in the current directory)
 *
 * @author Bruce Parrello
 *
 */
public class HammerTestProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(HammerTestProcessor.class);
    /** first genome */
    private Genome genome1;
    /** second genome */
    private Genome genome2;
    /** definitions for roles of interest */
    private RoleMap roleMap;
    /** DNA translator for genome 1 */
    private DnaTranslator xlate1;
    /** DNA translator for genome 2 */
    private DnaTranslator xlate2;

    // COMMAND-LINE OPTIONS

    /** name of the role definition file */
    @Option(name = "--roles", metaVar = "sours.definition", usage = "role definition file")
    private File roleFile;

    /** name of the first genome GTO */
    @Argument(index = 0, metaVar = "g1.gto", usage = "name of the first GTO file")
    private File gto1File;

    /** name of the second geome GTO */
    @Argument(index = 1, metaVar = "g2.gto", usage = "name of the second GTO file")
    private File gto2File;

    @Override
    protected void setDefaults() {
        this.roleFile = new File(System.getProperty("user.dir"), "roles.for.hammers");
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        if (! this.gto1File.canRead())
            throw new FileNotFoundException("GTO file " + this.gto1File + " is not found or unreadable.");
        if (! this.gto2File.canRead())
            throw new FileNotFoundException("GTO file " + this.gto1File + " is not found or unreadable.");
        this.genome1 = new Genome(this.gto1File);
        this.xlate1 = new DnaTranslator(this.genome1.getGeneticCode());
        this.genome2 = new Genome(this.gto2File);
        this.xlate2 = new DnaTranslator(this.genome2.getGeneticCode());
        if (! this.roleFile.canRead())
            throw new FileNotFoundException("Role file " + this.roleFile + " is not found or unreadable.");
        this.roleMap = RoleMap.load(this.roleFile);
        log.info("{} roles will be analyzed.", this.roleMap.size());
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        // Get each genome's roles.
        Map<String, List<Feature>> role1Map = this.getFeatures(this.genome1);
        Map<String, List<Feature>> role2Map = this.getFeatures(this.genome2);
        // Compare the roles.
        for (var role1Entry : role1Map.entrySet()) {
            String role = role1Entry.getKey();
            List<Feature> feats1 = role1Entry.getValue();
            List<Feature> feats2 = role2Map.get(role);
            // First, protein distance.  Get protein kmer lists for each genome.
            List<ProteinKmers> prots1 = feats1.stream().map(x -> new ProteinKmers(x.getProteinTranslation(), 8)).collect(Collectors.toList());
            List<ProteinKmers> prots2 = feats2.stream().map(x -> new ProteinKmers(x.getProteinTranslation(), 8)).collect(Collectors.toList());
            int bestSim = 0;
            for (ProteinKmers prot1 : prots1) {
                for (ProteinKmers prot2 : prots2) {
                    int sim = prot1.similarity(prot2);
                    if (sim > bestSim) bestSim = sim;
                }
            }
            log.info("Protein similarity for {} is {}.", role, bestSim);
            // Now, DNA distance.  Create RNA kmers for each genome.
            RnaKmers dna1 = new RnaKmers(20);
            feats1.stream().forEach(x -> dna1.addSequence(x.getDna()));
            RnaKmers dna2 = new RnaKmers(20);
            feats2.stream().forEach(x -> dna2.addSequence(x.getDna()));
            log.info("Dna similarity for {} is {}.", role, dna1.similarity(dna2));
            // Now validate each feature.
            feats1.stream().forEach(x -> this.validateFeature(x, xlate1));
            feats2.stream().forEach(x -> this.validateFeature(x, xlate2));
        }

    }

    /**
     * Validate the protein translation in the specified feature.
     *
     * @param feat		feature to validate
     * @param xlate		DNA translator to use
     */
    private void validateFeature(Feature feat, DnaTranslator xlate) {
        String dna = feat.getDna();
        String translated = xlate.pegTranslate(dna);
        String prot = feat.getProteinTranslation();
        // Remove the stop codon on the translation.
        translated = StringUtils.removeEnd(translated, "*");
        if (! prot.contentEquals(translated)) {
            ProteinKmers protK = new ProteinKmers(prot, 8);
            ProteinKmers translateK = new ProteinKmers(translated, 8);
            int sim = protK.similarity(translateK);
            log.warn("Protein mismatch for {}:  similarity = {}.", feat.getId(), sim);
        }
    }

    /**
     * Locate the features for each role in the role map.
     *
     * @param genome	genome containing the features
     *
     * @return a map from role IDs to feature lists
     */
    private Map<String, List<Feature>> getFeatures(Genome genome) {
        var retVal = new HashMap<String, List<Feature>>(this.roleMap.size() * 4 / 3 + 1);
        for (Feature peg : genome.getPegs()) {
            var roles = peg.getUsefulRoles(this.roleMap);
            for (Role role : roles) {
                List<Feature> featList = retVal.computeIfAbsent(role.getId(), x -> new ArrayList<Feature>(5));
                featList.add(peg);
            }
        }
        return retVal;
    }

}
