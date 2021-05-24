/**
 *
 */
package org.theseed.p3api.common;

import java.util.Comparator;

import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.locations.Location;
import org.theseed.reports.NaturalSort;
import org.theseed.sequence.blast.BlastHit;
import org.theseed.utils.IDescribable;

/**
 * This object describes an SSU rRNA hit.  It includes the genome ID and name, the location in the genome, and a description.
 * Methods are provided to create a descriptor from an RNA feature and a BLAST hit.  Within genome, the descriptors are
 * ordered by location so that we can easily detect overlapping hits from the different detection methods.
 *
 * @author Bruce Parrello
 *
 */
public class RnaDescriptor implements Comparable<RnaDescriptor> {

    /**
     * Enumerator for type of hit
     */
    public static enum Type implements IDescribable {
        BLAST {
            @Override
            public String getDescription() {
                return "Blast Hit";
            }
        }, ANNOTATION {
            @Override
            public String getDescription() {
                return "RNA Annotation";
            }
        };

        @Override
        public abstract String getDescription();

    }

    // FIELDS
    /** ID of the genome containing the RNA sequence */
    private String genomeId;
    /** name of the genome containing the RNA sequence */
    private String genomeName;
    /** location in the genome of the RNA hit */
    private Location loc;
    /** type of hit */
    private Type type;
    /** descriptive string */
    private String description;
    /** comparator for locations */
    private Comparator<Location> STRAND_COMPARE = new Location.StrandSorter();
    /** comparator for genome IDs */
    private Comparator<String> GENOME_COMPARE = new NaturalSort();

    /**
     * Construct a descriptor for a blast hit.
     *
     * @param genome	genome containing the hit
     * @param hit		blast hit evidence for the hit
     */
    public RnaDescriptor(Genome genome, BlastHit hit) {
        // Save the genome information.
        this.genomeId = genome.getId();
        this.genomeName = genome.getName();
        // Compute the hit location in the genome.
        this.loc = hit.getQueryLoc();
        if (this.loc.getDir() != hit.getSubjectLoc().getDir())
            this.loc = this.loc.reverse();
        // Save the type and description.
        this.type = Type.BLAST;
        this.description = hit.getSubjectDef();
    }

    /**
     * Construct a descriptor for an annotation hit.
     *
     * @param genome	genome containing the annotation
     * @param feat		annotated RNA feature
     */
    public RnaDescriptor(Genome genome, Feature feat) {
        // Save the genome information.
        this.genomeId = genome.getId();
        this.genomeName = genome.getName();
        // Get the feature location.
        this.loc = feat.getLocation();
        // Save the type and description.
        this.type = Type.ANNOTATION;
        this.description = feat.getId();
    }

    /**
     * Check a new descriptor against this one.  If they overlap, merge the locations and return TRUE.
     * Otherwise, return FALSE.
     *
     * @param other		other descriptor to check
     * <
     * @return TRUE if the descriptor was merged with this one, else FALSE
     */
    public boolean checkForMerge(RnaDescriptor other) {
        boolean retVal = false;
        if (this.genomeId == other.genomeId && this.type == other.type) {
            // Same genome and type, so check for overlapping locations.
            if (this.loc.isSameStrand(other.loc) && this.loc.isOverlapping(other.loc)) {
                // Here the locations overlap, so merge them.
                this.loc.merge(other.loc);
                retVal = true;
            }
        }
        return retVal;
    }

    @Override
    public int compareTo(RnaDescriptor o) {
        int retVal = GENOME_COMPARE.compare(this.genomeId, o.genomeId);
        if (retVal == 0) {
            retVal = STRAND_COMPARE.compare(this.loc, o.loc);
            if (retVal == 0) {
                retVal = this.type.compareTo(o.type);
                if (retVal == 0)
                    retVal = this.description.compareTo(o.description);
            }
        }
        return retVal;
    }

    /**
     * @return the header for a report containing RNA descriptors
     */
    public static String getHeader() {
        return "genome_id\tgenome_name\tlength\tlocation\ttype\tdescription";
    }

    /**
     * @return the output line for this RNA descriptor
     */
    public String output() {
        return String.format("%s\t%s\t%d\t%s\t%s\t%s", this.genomeId, this.genomeName, this.loc.getLength(), this.loc.toString(),
                this.type.getDescription(), this.description);
    }

}
