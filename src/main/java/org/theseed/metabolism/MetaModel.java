/**
 *
 */
package org.theseed.metabolism;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Genome;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonException;
import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;

/**
 * This object represents a metabolic model loaded from a JSON file.  The model consists of
 * nodes that represent chemical products and reactions that are triggered by genes.  The
 * primary goal is to determine the effect of suppressing or over-stimulating individual
 * genes.
 *
 * @author Bruce Parrello
 *
 */
public class MetaModel {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MetaModel.class);
    /** original json object body */
    private JsonObject modelObject;
    /** name of map */
    private String mapName;
    /** genome on which the model is based */
    private Genome baseGenome;
    /** map of FIG IDs to reactions */
    private Map<String, Set<Reaction>> reactionMap;
    /** set of all reactions not associated with features */
    private Set<Reaction> orphans;
    /** map of metabolite BiGG IDs to nodes */
    private Map<String, List<ModelNode.Metabolite>> metaboliteMap;
    /** map of node IDs to nodes */
    private Map<Integer, ModelNode> nodeMap;
    /** return value when no reactions found */
    private Set<Reaction> NO_REACTIONS = Collections.emptySet();
    /** return value when no metabolite nodes are found */
    private List<ModelNode.Metabolite> NO_METABOLITES = Collections.emptyList();

    /**
     * Construct a metabolic model from a file and a genome.
     *
     * @param inFile	name of the file containing the model JSON
     * @param genome	genome on which the model was based
     * @throws IOException
     */
    public MetaModel(File inFile, Genome genome) throws IOException {
        this.baseGenome = genome;
        FileReader reader = new FileReader(inFile);
        try {
            JsonArray parts = (JsonArray) Jsoner.deserialize(reader);
            this.modelObject = (JsonObject) parts.get(1);
            // Get the map name.
            JsonObject metaObject = (JsonObject) parts.get(0);
            String name = (String) metaObject.get("map_name");
            if (name == null)
                name = "Metabolic map for " + genome.toString();
            this.mapName = name;
        } catch (JsonException e) {
            throw new IOException("JSON error in " + inFile + ":" + e.toString());
        }
        // Now we want to build the reaction hash.  First, we need a map of aliases
        // to FIG IDs.
        var aliasMap = genome.getAliasMap();
        // Now we loop through the reactions, creating the map.
        JsonObject reactions = (JsonObject) this.modelObject.get("reactions");
        int nReactions = reactions.size();
        log.info("{} reactions found in map {}.", nReactions, this.mapName);
        final int hashSize = reactions.size() * 4 / 3 + 1;
        this.reactionMap = new HashMap<String, Set<Reaction>>(hashSize);
        this.orphans = new HashSet<Reaction>();
        for (Map.Entry<String, Object> reactionEntry : reactions.entrySet()) {
            int reactionId = Integer.valueOf(reactionEntry.getKey());
            JsonObject reactionObject = (JsonObject) reactionEntry.getValue();
            Reaction reaction = new Reaction(reactionId, reactionObject);
            Collection<String> genes = reaction.getGenes();
            // For each gene alias, connect this reaction to the relevant features.
            boolean found = false;
            for (String gene : genes) {
                var fids = aliasMap.get(gene);
                if (fids == null)
                    log.warn("No features found for gene alias \"" + gene + "\" in reaction " + reaction.toString());
                else {
                    for (String fid : fids) {
                        Set<Reaction> fidReactions = this.reactionMap.computeIfAbsent(fid, x -> new TreeSet<Reaction>());
                        fidReactions.add(reaction);
                        found = true;
                    }
                }
            }
            // If we did not connect this reaction to a gene, make it an orphan.
            if (! found)
                this.orphans.add(reaction);
        }
        // Now set up the nodes.
        JsonObject nodes = (JsonObject) this.modelObject.get("nodes");
        this.nodeMap = new HashMap<Integer, ModelNode>(nodes.size() * 4 / 3 + 1);
        this.metaboliteMap = new HashMap<String, List<ModelNode.Metabolite>>(nodes.size());
        for (Map.Entry<String, Object> nodeEntry : nodes.entrySet()) {
            int nodeId = Integer.valueOf(nodeEntry.getKey());
            ModelNode node = ModelNode.create(nodeId, (JsonObject) nodeEntry.getValue());
            this.nodeMap.put(nodeId, node);
            if (node instanceof ModelNode.Metabolite) {
                ModelNode.Metabolite metaNode = (ModelNode.Metabolite) node;
                List<ModelNode.Metabolite> metaNodes = this.metaboliteMap.computeIfAbsent(metaNode.getBiggId(),
                        x -> new ArrayList<ModelNode.Metabolite>());
                metaNodes.add(metaNode);
            }
        }
    }

    /**
     * @return the model object
     */
    public JsonObject getModelObject() {
        return this.modelObject;
    }

    /**
     * @return the base genome
     */
    public Genome getBaseGenome() {
        return this.baseGenome;
    }

    /**
     * @return the number of features with reactions
     */
    public int featuresCovered() {
        return this.reactionMap.size();
    }

    /**
     * @return the map name
     */
    public String getMapName() {
        return this.mapName;
    }

    /**
     * @return the reactions for the specified feature
     *
     * @param fid	feature whose reactions are desired
     */
    public Set<Reaction> getReactions(String fid) {
        Set<Reaction> retVal = this.reactionMap.get(fid);
        if (retVal == null)
            retVal = NO_REACTIONS;
        return retVal;
    }

    /**
     * @return the reaction map for this model
     */
    public Map<String, Set<Reaction>> getReactionMap() {
        return this.reactionMap;
    }

    /**
     * @return the set of all reactions
     */
    public Set<Reaction> getAllReactions() {
        Set<Reaction> retVal = this.reactionMap.values().stream().flatMap(x -> x.stream()).collect(Collectors.toSet());
        retVal.addAll(this.orphans);
        return retVal;
    }

    /**
     * @return the set of all orphan reactions
     */
    public Set<Reaction> getOrphanReactions() {
        return this.orphans;
    }

    /**
     * @return the node with the specified ID, or NULL if the node is not found
     *
     * @param nodeId		ID of the node to return
     */
    public ModelNode getNode(int nodeId) {
        return this.nodeMap.get(nodeId);
    }

    /**
     * @return the nodes for the specified metabolite
     *
     * @param bigg_id	BiGG ID of the desired metabolite
     */
    public List<ModelNode.Metabolite> getMetabolites(String bigg_id) {
        List<ModelNode.Metabolite> retVal = this.metaboliteMap.get(bigg_id);
        if (retVal == null)
            retVal = NO_METABOLITES;
        return retVal;
    }

    /**
     * @return the primary node for the specified metabolite, or NULL if there is none
     *
     * @param bigg_id	BiGG ID of the desired metabolite
     */
    public ModelNode.Metabolite getPrimary(String bigg_id) {
        ModelNode.Metabolite retVal = null;
        List<ModelNode.Metabolite> list = this.metaboliteMap.get(bigg_id);
        for (ModelNode.Metabolite node : list) {
            if (node.isPrimary())
                retVal = node;
        }
        return retVal;
    }

    /**
     * @return the number of metabolites
     */
    public int getMetaboliteCount() {
        return this.metaboliteMap.size();
    }

    /**
     * @return the number of nodes
     */
    public int getNodeCount() {
        return this.nodeMap.size();
    }

    /**
     * @return the map of metabolites to metabolite nodes
     */
    public Map<String, List<ModelNode.Metabolite>> getMetaboliteMap() {
        return this.metaboliteMap;
    }

}
