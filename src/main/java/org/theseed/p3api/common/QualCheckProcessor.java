/**
 *
 */
package org.theseed.p3api.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.sequence.fastq.FastqSampleGroup;
import org.theseed.sequence.fastq.SeqRead;
import org.theseed.utils.BaseProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a quick-and-dirty command to compute the mean quality of a FASTQ sample group.  The positional parameter
 * is the sample group file name.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type	type of sample group (default FASTQ)
 *
 * @author Bruce Parrello
 *
 */
public class QualCheckProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(QualCheckProcessor.class);

    // COMMAND-LINE OPTIONS

    /** type of sample group */
    @Option(name = "--type", usage = "type of FASTQ sample group")
    private FastqSampleGroup.Type groupType;

    /** sample group directory */
    @Argument(index = 0, metaVar = "inDir", usage = "name of the sample group master directory")
    private File inDir;

    @Override
    protected void setDefaults() {
        this.groupType = FastqSampleGroup.Type.FASTQ;
    }

    @Override
    protected boolean validateParms() throws IOException, ParseFailureException {
        // Get a filter for this group type.
        FileFilter filter = this.groupType.getFilter();
        if (! filter.accept(this.inDir))
            throw new FileNotFoundException(this.inDir + " is not found or not a valid FASTQ sample group directory of type "
                    + this.groupType.toString());
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        try (FastqSampleGroup sampleGroup = this.groupType.create(this.inDir)) {
            Set<String> samples = sampleGroup.getSamples();
            log.info("{} samples found in {}.", samples.size(), this.inDir);
            int seqCount = 0;
            double totalQual = 0.0;
            for (String sample : samples) {
                log.info("Processing sample {}.", sample);
                var reads = sampleGroup.sampleIter(sample);
                int sampCount = 0;
                for (SeqRead read : reads) {
                    totalQual += read.getQual();
                    seqCount++;
                    sampCount++;
                    if (log.isInfoEnabled() && sampCount % 10000 == 0)
                        log.info("{} sequences read in {}.", seqCount, sample);
                }
            }
            if (seqCount == 0)
                log.info("No sequences found in {}.", this.inDir);
            else
                log.info("Mean quality in {} sequences and {} samples is {}.", seqCount, samples.size(), totalQual / seqCount);
        }

    }

}
