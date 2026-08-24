package org.theseed.p3api.common;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.counters.CountMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.utils.BaseMultiReportProcessor;
import org.theseed.utils.IntegerList;


/**
 * This command reads the output of the functional role mapping command (kmers.anno funMap) and produces a report
 * on how complex the mapping is. For each functional role, we classify it as Unchanged (same in old and new annotation
 * systems), mostly Unchanged, Simple (mapped to a new annotation), or mostly Simple. The "mostly" will actually be
 * represented by a percentage range, as determined by the command-line options. Finally, a function is classified as
 * Vague if it does not map to any single function more than the percentage indicated by the lowest "mostly" value. 
 * Finally, the report will count the number of distinct mappings for each function and the total number of occurrences. 
 * In addition to the main report, a statistics file will be generated containing totals for each major category and the 
 * number of functions in that category.
 * 
 * The input from the functional role mapping command will be taken from the standard input. The command-line options are as follows.
 * 
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -D	output directory name (default "funReports" in the current directory)
 * -i   input file containing the output of the functional role mapping command (if not STDIN)
 *
 * --clear	    erase the output directory before processing
 * --limits     percentage limits for the "mostly" categories (default 90,80,70,50)
 * 
 * FunReportProcessor
 */
public class FunReportProcessor extends BaseMultiReportProcessor {

    // FIELDS
    /** logging facility */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FunReportProcessor.class);
    /** array of category total objects for the summary report */
    private CategoryTotals[] categoryTotals;
    /** input file stream */
    private TabbedLineReader inputStream;
    /** input column for old functional role */
    private int oldCol;
    /** input column for new functional role */
    private int newCol;
    /** input column for occurrence count */
    private int countCol;

    // COMMAND-LINE OPTIONS

    /** percentage limits for the "mostly" categories */
    @Option(name = "--limits", metaVar = "95,80,50,40", usage = "percentage limits for the mapping integrity categories")
    private String limits;

    /** input file name (if not STDIN) */
    @Option(name = "-i", metaVar = "funMap.tbl", usage = "input file containing the output of the functional role mapping command (if not STDIN)")
    private File inputFile;


    // UTILITY CLASSES

    /**
     * This object is used to track the totals for the statistics report. We keep these objects in an array and once
     * we've found the "best match" for a particular function, we add its counts.
     */
    protected static class CategoryTotals {
        
        /** lower limit (inclusive) for the most common outbound value's mapping fraction to be in this category */
        private final double lowerLimit;
        /** TRUE if this category should be used when the most common outbound value is the identity */
        private final boolean identityFlag;
        /** name to display for this category */
        private final String name;
        /** number of functions in this category */
        private int functionCount;
        /** total number of occurrences of functions in this category */
        private int occurrenceCount;
        /** total number of distinct outbound mapping values (other than identity) for functions in this category */
        private int mappingCount;
        /** total number of occurrences that did not map to the most common outbound value */
        private int otherCount;

        /**
         * Construct a new category totals object.
         * 
         * @param lower		lower limit for the most common outbound value's mapping fraction to be in this category
         * @param identity	TRUE if this category should be used when the most common outbound value is the identity
         */
        public CategoryTotals(int lower, boolean identity) {
            this.lowerLimit = lower / 100.0;
            this.identityFlag = identity;
            this.functionCount = 0;
            this.occurrenceCount = 0;
            this.mappingCount = 0;
            this.otherCount = 0;
            // Now we compute the name. The name depends on the lower limit and the identity
            // flag. For a lower limit of 0 we use "Vague".
            StringBuilder nameBuilder = new StringBuilder(20);
            if (lowerLimit == 0.0)
                nameBuilder.append("Vaguely ");
            else {
                // Here we have a non-zero lower limit. We need to convert it to a percentage.
                nameBuilder.append(String.format("%6d%%-", lower));
            }
            if (identityFlag)
                nameBuilder.append("Unchanged");
            else
                nameBuilder.append("Changed");
            this.name = nameBuilder.toString();
        }

        /**
         * Check a function against this category to see if it belongs here. If it does, we add its counts to the totals.
         * 
         * @param identity      TRUE if the most common outbound value is the identity
         * @param occurrences	number of occurrences of the function being added
         * @param mappings		number of distinct outbound non-identity mapping values for the function being added
         * @param others    	number of occurrences that did not map to the most common outbound value
         * 
         * @return TRUE if the function belongs in this category
         * 
         */
        public boolean checkFunction(boolean identity, int occurrences, int mappings, int others) {
            boolean retVal;
            // Compute the fraction of occurrences that map to the most common outbound value.
            double fraction = 1.0 - ((double) others / (double) occurrences);
            if (fraction >= this.lowerLimit && identity == this.identityFlag) {
                this.addFunction(occurrences, mappings, others);
                retVal = true;
            } else {
                retVal = false;
            }
            return retVal;
        }

        /**
         * Add a function to this category.
         *
         * @param occurrences	number of occurrences of the function being added
         * @param mappings		number of distinct outbound non-identity mapping values for the function being added
         * @param others    	number of occurrences that did not map to the most common outbound value
         */
        private void addFunction(int occurrences, int mappings, int others) {
            this.functionCount++;
            this.occurrenceCount += occurrences;
            this.mappingCount += mappings;
            this.otherCount += others;
        }

        /**
         * Write the data line for this category to the output.
         * 
         * @param writer	output stream for the report
         * 
         */
        public void writeLine(PrintWriter writer) {
            writer.format("%s\t%10d\t%10d\t%10.3f\t%10d\t%10.3f%n", this.name, this.functionCount, this.occurrenceCount, ((double) this.mappingCount) / (double) this.functionCount, 
                    this.otherCount, ((double) this.otherCount) * 100.0 / (double) this.occurrenceCount);
        }

        /**
         * Write the header for the statistics report.
         * 
         * @param writer    output stream for the report
         */
        public static void writeHeader(PrintWriter writer) {
            writer.println("category\tfunctions\ttotal\ttargets\tuncommon\t% uncommon");
        }

        /**
         * @return the name of this category
         */
        public String getName() {
            return this.name;
        }

    }


    @Override
    protected File setDefaultOutputDir(File curDir) {
        return new File(curDir, "funReports");
    }

    @Override
    protected void setMultiReportDefaults() {
        this.limits = "90,80,70,50";
        this.inputFile = null;
    }

    @Override
    protected void validateMultiReportParms() throws IOException, ParseFailureException {
        // Parse the limits string into an array of category total objects. This process will throw a NumberFormatException
        // if the limits string is not valid.
        IntegerList limitList = new IntegerList(this.limits);
        if (limitList.size() < 1)
            throw new ParseFailureException("At least one limit must be specified.");
        this.categoryTotals = new CategoryTotals[limitList.size() * 2 + 4];
        // Do a little fancy dancing to skip "100" if the user coded it.
        int inIndex = (limitList.get(0) == 100 ? 1 : 0);
        // Add the 100% categories.
        this.categoryTotals[0] = new CategoryTotals(100, true);
        this.categoryTotals[1] = new CategoryTotals(100, false);
        int outIndex = 2;
        int oldLimit = 100;
        // Now add the user-specified limits.
        while (inIndex < limitList.size()) {
            int limit = limitList.get(inIndex);
            if (limit >= oldLimit || limit < 0)
                throw new ParseFailureException("Limits must be in decreasing order and non-negative.");
            this.categoryTotals[outIndex++] = new CategoryTotals(limit, true);
            this.categoryTotals[outIndex++] = new CategoryTotals(limit, false);
            oldLimit = limit;
            inIndex++;
        }
        // Finally, add the 0% category.
        this.categoryTotals[outIndex++] = new CategoryTotals(0, true);
        this.categoryTotals[outIndex++] = new CategoryTotals(0, false);
        // With all the totals set up, we can now open the input stream.
        if (this.inputFile == null) {
            log.info("Reading functional role mapping data from standard input.");
            this.inputStream = new TabbedLineReader(System.in);
        } else {
            log.info("Reading functional role mapping data from {}.", this.inputFile);
            this.inputStream = new TabbedLineReader(this.inputFile);
        }
        // Validate the input file by locating the input columns.
        this.oldCol = this.inputStream.findField("old_function");
        this.newCol = this.inputStream.findField("new_function");
        this.countCol = this.inputStream.findField("count");
    }

    @Override
    protected void runMultiReports() throws Exception {
        // We use a try block to ensure the input stream is closed when we're done.
        try {
            // Open the detail output file.
            try (PrintWriter detailWriter = this.openReport("functions.tbl")) {
                log.info("Producing detail report.");
                // Write the header for the detail report.
                detailWriter.println("old_function\tcategory\tbest_new_function\tmappings\toccurrences\tcommon\tuncommon");
                // Each old function group begins with an identity mapping, then all the other outbound mappings. When we hit an
                // identity mapping, we know we have finished the previous function and can process it.
                // The counts for each outbound value are stored in here, keyed by the outbound value.
                CountMap<String> countMap = new CountMap<>();
                String oldFunc = null;
                // We loop through the input file, showing progress every now and then.
                int linesIn = 0;
                int funsIn = 0;
                for (var line : this.inputStream) {
                    linesIn++;
                    String thisFunc = line.get(this.oldCol);
                    String newFunc = line.get(this.newCol);
                    if (newFunc.isBlank()) {
                        // Here we are starting a new function. If we have an old function, we need to process it.
                        if (oldFunc != null) {
                            funsIn++;
                            this.processFunction(oldFunc, countMap, detailWriter);
                            countMap.deleteAll();
                        }
                        oldFunc = thisFunc;
                        newFunc = "(unchanged)";
                    } else if (! thisFunc.equals(oldFunc))
                        throw new ParseFailureException("Input file is not sorted by old function.");
                    // Now we have the old and new functions. Count the number of times we mapped to the new function.          
                    int count = line.getInt(this.countCol);
                    countMap.count(newFunc, count);
                    if (linesIn % 10000 == 0)
                        log.info("{} lines read, {} functions processed.", linesIn, funsIn);
                }
                // Process the last function.
                if (oldFunc != null) {
                    this.processFunction(oldFunc, countMap, detailWriter);
                    funsIn++;
                }
                log.info("{} lines read, {} functions processed.", linesIn, funsIn);
            }
            // Now we can produce the summary report.
            try (PrintWriter summaryWriter = this.openReport("summary.tbl")) {
                log.info("Producing summary report.");
                CategoryTotals.writeHeader(summaryWriter);
                for (CategoryTotals cat : this.categoryTotals)
                    cat.writeLine(summaryWriter);
            }
        } finally {
            this.inputStream.close();
        }

    }

    /**
     * Write the output line for a single function and update the category totals.
     * 
     * @param oldFunc           old functional role
     * @param countMap          count map of new functional roles and their occurrence counts
     * @param detailWriter      output stream for the detail report
     */
    private void processFunction(String oldFunc, CountMap<String> countMap, PrintWriter detailWriter) {
        // First, we must compute the category. We do this by finding the most common outbound value and its count, then 
        // computing the other totals for this function.
        List<CountMap<String>.Count> counts = countMap.sortedCounts();
        String bestFunc = counts.get(0).getKey();
        int bestCount = counts.get(0).getCount();
        int totalCount = countMap.getTotal();
        int otherCount = totalCount - bestCount;
        int mappingCount = counts.size() - 1;  // don't count the identity mapping
        boolean identity = bestFunc.equals("(unchanged)");
        // Now we can find the category for this function. We do this by looping through the category totals and checking each one.
        int idx = 0;
        CategoryTotals cat = null;
        while (idx < this.categoryTotals.length && cat == null) {
            CategoryTotals testCat = this.categoryTotals[idx];
            if (testCat.checkFunction(identity, totalCount, mappingCount, otherCount))
                cat = testCat;
            idx++;
        }
        if (cat == null)
            log.error("Function {} did not fit into any category.", oldFunc);
        else {
            // Here we found the category. Write the output line and we're done.
            detailWriter.format("%s\t%s\t%s\t%d\t%d\t%d\t%d%n", oldFunc, cat.getName(), bestFunc, mappingCount, totalCount, bestCount, otherCount);
        }
    }
 
}
