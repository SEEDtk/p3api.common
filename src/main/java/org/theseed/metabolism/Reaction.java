/**
 *
 */
package org.theseed.metabolism;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonKey;
import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This object represents a reaction present in a metabolic model.  The reaction contains
 * segments that describe the nodes to which it connects, the list of genes that trigger it,
 * various bits of identifiying information, and the stoichiometry of the compounds involved.
 *
 * @author Bruce Parrello
 *
 */
public class Reaction implements Comparable<Reaction> {

    // FIELDS
    /** ID number of this reaction */
    private int id;
    /** name of the reaction */
    private String name;
    /** BiGG identifier */
    private String biggId;
    /** rule for triggering the reaction (text based on BiGG IDs) */
    private String reactionRule;
    /** reversibility flag */
    private boolean reversible;
    /** list of gene aliases */
    private Set<String> aliases;
    /** metabolite list */
    private List<Stoich> metabolites;
    /** connections list */
    private List<Segment> segments;
    /** display location of reaction label */
    private Coordinate labelLoc;

    private static enum ReactionKeys implements JsonKey {
        NAME("<unknown>"), BIGG_ID(""), REVERSIBILITY(false), LABEL_X(0.0), LABEL_Y(0.0),
        GENE_REACTION_RULE(""), COEFFICIENT(1);

        private final Object m_value;

        private ReactionKeys(final Object value) {
            this.m_value = value;
        }

        /** This is the string used as a key in the incoming JsonObject map.
         */
        @Override
        public String getKey() {
            return this.name().toLowerCase();
        }

        /** This is the default value used when the key is not found.
         */
        @Override
        public Object getValue() {
            return this.m_value;
        }

    }

    private static enum SegmentKeys implements JsonKey {
        FROM_NODE_ID(0), TO_NODE_ID(0);

        private final Object m_value;

        private SegmentKeys(final Object value) {
            this.m_value = value;
        }

        /** This is the string used as a key in the incoming JsonObject map.
         */
        @Override
        public String getKey() {
            return this.name().toLowerCase();
        }

        /** This is the default value used when the key is not found.
         */
        @Override
        public Object getValue() {
            return this.m_value;
        }

    }

    /**
     * This is a simple object to represent stoichiometry.  The sort order puts reactants
     * before products.
     */
    public static class Stoich implements Comparable<Stoich> {

        /** stoichiometric coefficient */
        private int coefficient;
        /** BiGG identifier of metabolite */
        private String biggId;

        /**
         * Construct a new stoichiometric representation.
         *
         * @param coeff		coefficient
         * @param bigg		BiGG ID for metabolite
         */
        public Stoich(int coeff, String bigg) {
            this.coefficient = coeff;
            this.biggId = bigg;
        }

        @Override
        public int compareTo(Stoich o) {
            int retVal = this.coefficient - o.coefficient;
            if (retVal == 0)
                retVal = this.biggId.compareTo(o.biggId);
            return retVal;
        }

        /**
         * @return the coefficient (always positive)
         */
        public int getCoeff() {
            return (this.coefficient < 0 ? -this.coefficient : this.coefficient);
        }

        /**
         * @return TRUE for a product, FALSE for a reactant
         */
        public boolean isProduct() {
            return (this.coefficient > 0);
        }

        @Override
        public String toString() {
            int coeff = this.getCoeff();
            String retVal;
            if (coeff == 1)
                retVal = this.biggId;
            else
                retVal = String.format("%d*%s", coeff, this.biggId);
            return retVal;
        }

    }

    /**
     * This class represents a connection made by a reaction.  It contains the IDs of the
     * source and destination nodes, and the display coordinates (if any).
     * @author Bruce Parrello
     *
     */
    public static class Segment {

        /** ID of origin node */
        private int fromNode;
        /** ID of terminal node */
        private int toNode;

        /**
         * Construct a segment from a JSON object.
         *
         * @param segObject		source JSON object for the segment
         */
        public Segment(JsonObject segObject) {
            this.fromNode = segObject.getIntegerOrDefault(SegmentKeys.FROM_NODE_ID);
            this.toNode = segObject.getIntegerOrDefault(SegmentKeys.TO_NODE_ID);
        }

        /**
         * @return the from-node ID number
         */
        public int getFromNode() {
            return this.fromNode;
        }

        /**
         * @return the to-node ID number
         */
        public int getToNode() {
            return this.toNode;
        }

    }

    /**
     * Construct a reaction from a JSON object.
     *
     * @param id				ID of this reaction
     * @param reactionObject	JSON object containing the reaction
     */
    public Reaction(int id, JsonObject reactionObject) {
        this.id = id;
        this.name = reactionObject.getStringOrDefault(ReactionKeys.NAME);
        this.biggId = reactionObject.getStringOrDefault(ReactionKeys.BIGG_ID);
        this.reversible = reactionObject.getBooleanOrDefault(ReactionKeys.REVERSIBILITY);
        this.labelLoc = new Coordinate(reactionObject.getDoubleOrDefault(ReactionKeys.LABEL_X),
                reactionObject.getDoubleOrDefault(ReactionKeys.LABEL_Y));
        this.reactionRule = reactionObject.getStringOrDefault(ReactionKeys.GENE_REACTION_RULE);
        JsonArray geneList = (JsonArray) reactionObject.get("genes");
        // For the gene list, we extract all the aliases and store them as a set.
        this.aliases = new TreeSet<String>();
        if (geneList != null) {
            for (int i = 0; i < geneList.size(); i++) {
                JsonObject gene = (JsonObject) geneList.get(i);
                String bigg_id = (String) gene.get("bigg_id");
                String name = (String) gene.get("name");
                if (! StringUtils.isBlank(bigg_id))
                    this.aliases.add(bigg_id);
                if (! StringUtils.isBlank(name))
                    this.aliases.add(name);
            }
        }
        // For the metabolites list, we convert each one to a stoichiometry.
        List<Stoich> reactionParts;
        JsonArray metaList = (JsonArray) reactionObject.get("metabolites");
        if (metaList == null)
            reactionParts = Collections.emptyList();
        else {
            reactionParts = new ArrayList<Stoich>();
            // Get all the pieces.
            for (int i = 0; i < metaList.size(); i++) {
                JsonObject meta = (JsonObject) metaList.get(i);
                Stoich stoich = new Stoich(meta.getIntegerOrDefault(ReactionKeys.COEFFICIENT),
                        meta.getStringOrDefault(ReactionKeys.BIGG_ID));
                reactionParts.add(stoich);
            }
            // Sort them into a list.
            Collections.sort(reactionParts);
        }
        this.metabolites = reactionParts;
        // Finally, we process the segments.
        JsonObject segList = (JsonObject) reactionObject.get("segments");
        if (segList == null)
            this.segments = Collections.emptyList();
        else {
            this.segments = new ArrayList<Segment>();
            for (Object segItem : segList.values()) {
                Segment seg = new Segment((JsonObject) segItem);
                this.segments.add(seg);
            }
        }
    }

    /**
     * @return the reaction ID number
     */
    public int getId() {
        return this.id;
    }

    /**
     * @return the reaction's role name
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the BiGG ID for the reaction
     */
    public String getBiggId() {
        return this.biggId;
    }

    /**
     * @return the rule for triggering the reaction
     */
    public String getReactionRule() {
        return this.reactionRule;
    }

    /**
     * @return TRUE if this reaction is reversible
     */
    public boolean isReversible() {
        return this.reversible;
    }

    /**
     * @return the aliases for the genes that relate to this reaction
     */
    public Set<String> getGenes() {
        return this.aliases;
    }

    /**
     * @return the components of the reaction (reactants and products with stoichometric coefficients)
     */
    public List<Stoich> getMetabolites() {
        return this.metabolites;
    }

    /**
     * @return the connection segments of this reaction
     */
    public List<Segment> getSegments() {
        return this.segments;
    }

    /**
     * @return the coordinates of the reaction label
     */
    public Coordinate getLabelLoc() {
        return this.labelLoc;
    }

    @Override
    public int compareTo(Reaction o) {
        return (this.id - o.id);
    }

    @Override
    public String toString() {
        return "Reaction " + this.id + "(" + this.getName() + ")";
    }

}
