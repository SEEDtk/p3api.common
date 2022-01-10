/**
 *
 */
package org.theseed.metabolism;

import com.github.cliftonlabs.json_simple.JsonKey;
import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This object represents a node in a metabolic model.  The node generally represents a particular
 * metabolite, though some nodes are markers.
 *
 *
 * @author Bruce Parrello
 *
 */
public abstract class ModelNode implements Comparable<ModelNode> {

    // FIELDS
    /** ID number of this node */
    private int id;
    /** location of node */
    private Coordinate loc;

    public static enum NodeKeys implements JsonKey {
        NODE_TYPE("marker"), X(0), Y(0), BIGG_ID(""),
        NAME("<unknown>"), NODE_IS_PRIMARY(false);

        private final Object m_value;

        private NodeKeys(final Object value) {
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
     * Construct the underlying node object from the json descriptor.
     *
     * @param nodeId			node ID number
     * @param jsonObject	json descriptor of this node
     */
    protected ModelNode(int nodeId, JsonObject jsonObject) {
        this.id = nodeId;
        this.loc = new Coordinate(jsonObject.getDoubleOrDefault(NodeKeys.X),
                jsonObject.getDoubleOrDefault(NodeKeys.Y));
    }

    /**
     * Construct a model node from a JSON descriptor.
     *
     * @param nodeId			node ID number
     * @param jsonObject	json descriptor of this node
     */
    public static ModelNode create(int nodeId, JsonObject jsonObject) {
        String type = jsonObject.getStringOrDefault(NodeKeys.NODE_TYPE);
        ModelNode retVal;
        if (type.contentEquals("metabolite"))
            retVal = new Metabolite(nodeId, jsonObject);
        else
            retVal = new Marker(nodeId, type, jsonObject);
        return retVal;
    }

    /**
     * This subclass represents a metabolite node.
     */
    public static class Metabolite extends ModelNode {

        /** BiGG identifier of metabolite */
        private String bigg_id;
        /** name of metabolite */
        private String name;
        /** TRUE if this is the primary node */
        private boolean primaryFlag;

        /**
         * Construct a metabolite node from the json descriptor.
         *
         * @param nodeId			node ID number
         * @param jsonObject
         */
        public Metabolite(int nodeId, JsonObject jsonObject) {
            super(nodeId, jsonObject);
            this.bigg_id = jsonObject.getStringOrDefault(NodeKeys.BIGG_ID);
            this.name = jsonObject.getStringOrDefault(NodeKeys.NAME);
            this.primaryFlag = jsonObject.getBooleanOrDefault(NodeKeys.NODE_IS_PRIMARY);
        }

        /**
         * @return the bigg_id
         */
        public String getBiggId() {
            return this.bigg_id;
        }

        /**
         * @return the name
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return TRUE if this is the primary node for the metabolite
         */
        public boolean isPrimary() {
            return this.primaryFlag;
        }

    }

    /**
     * This subclass describes a marker ndoe.
     */
    public static class Marker extends ModelNode {

        /** marker type */
        private String type;

        /**
         * Construct a marker node.
         *
         * @param nodeId			node ID number
         * @param type			node type
         * @param jsonObject	JSON object representing this node
         */
        public Marker(int nodeId, String type, JsonObject jsonObject) {
            super(nodeId, jsonObject);
            this.type = type;
        }

        /**
         * @return the node type
         */
        public String getType() {
            return this.type;
        }

    }

    @Override
    public int compareTo(ModelNode o) {
        return this.id - o.id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ModelNode)) {
            return false;
        }
        ModelNode other = (ModelNode) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }

    /**
     * @return the node ID number
     */
    public int getId() {
        return this.id;
    }

    /**
     * @return the coordinate location
     */
    public Coordinate getLoc() {
        return this.loc;
    }

}
