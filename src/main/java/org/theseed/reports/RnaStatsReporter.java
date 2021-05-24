/**
 *
 */
package org.theseed.reports;

import java.io.PrintWriter;

/**
 * This is the base class for RNA statistic reports.
 *
 * @author Bruce Parrello
 *
 */
public class RnaStatsReporter extends BaseWritingReporter {

    public RnaStatsReporter(PrintWriter writer) {
        super(writer);
    }

}
