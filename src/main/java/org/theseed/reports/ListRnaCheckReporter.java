/**
 *
 */
package org.theseed.reports;

import java.io.PrintWriter;
import java.util.SortedSet;
import java.util.TreeSet;

import org.theseed.genome.Genome;
import org.theseed.p3api.common.RnaDescriptor;

/**
 * This is a simple report that lists the RNA hits organized by genome and then location within genome.
 *
 * @author Bruce Parrello
 *
 */
public class ListRnaCheckReporter extends RnaCheckReporter {

    // FIELDS
    /** list of descriptors for the current genome */
    private SortedSet<RnaDescriptor> descriptors;

    public ListRnaCheckReporter(PrintWriter writer) {
        super(writer);
        this.descriptors = new TreeSet<RnaDescriptor>();
    }

    @Override
    public void openReport() {
        this.println(RnaDescriptor.getHeader());
    }

    @Override
    public void openGenome(Genome genome) {
        // Get ready to sort this genome's hits.
        this.descriptors.clear();
    }

    @Override
    public void recordHit(RnaDescriptor descriptor) {
        this.descriptors.add(descriptor);
    }

    @Override
    public void closeGenome(Genome genome) {
        // Put in a separator line.
        this.println();
        // Write out this genome's hits.
        for (RnaDescriptor descriptor : this.descriptors)
            this.println(descriptor.output());
    }

    @Override
    public void finish() {
    }

}
