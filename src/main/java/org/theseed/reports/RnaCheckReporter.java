/**
 *
 */
package org.theseed.reports;

import java.io.PrintWriter;

import org.theseed.genome.Genome;
import org.theseed.p3api.common.RnaDescriptor;

/**
 * This is the base class for reports from the RnaCheckProcesor.
 *
 * @author Bruce Parrello
 *
 */
public abstract class RnaCheckReporter extends BaseWritingReporter {

    /**
     * This enum is used to specify the type of report.
     */
    public static enum Type {
        LIST {
            @Override
            public RnaCheckReporter create(PrintWriter writer) {
                return new ListRnaCheckReporter(writer);
            }
        };

        /**
         * @return an RNA check reporter of this type
         *
         * @param writer	output writer to receive the report
         */
        public abstract RnaCheckReporter create(PrintWriter writer);
    }

    /**
     * Construct an RNA check reporter
     *
     * @param writer	output writer to receive the report
     */
    public RnaCheckReporter(PrintWriter writer) {
        super(writer);
    }

    /**
     * Initialize the report.
     */
    public abstract void openReport();

    /**
     * Begin processing for a genome.
     *
     * @param genome	genome of interest
     */
    public abstract void openGenome(Genome genome);

    /**
     * Record an RNA hit in a genome.
     *
     * @param descriptor	RNA hit description
     */
    public abstract void recordHit(RnaDescriptor descriptor);

    /**
     * Finish processing of the genome.
     *
     * @param genome	genome of interest
     */
    public abstract void closeGenome(Genome genome);

    /**
     * Complete the report.
     */
    public abstract void finish();

}
