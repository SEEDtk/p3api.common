/**
 *
 */
package org.theseed.p3api.common;

import java.io.IOException;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.utils.BaseProcessor;

/**
 * This is a simple command that echoes its parameters, to provide a quick test of calling into Java from PERL.
 *
 * @author Bruce Parrello
 *
 */
public class SimpleProcessor extends BaseProcessor {

    // FIELDS
    protected static Logger log = LoggerFactory.getLogger(SimpleProcessor.class);

    // COMMAND-LINE OPTIONS

    @Argument(index = 0, usage = "parameters  to echo", required = true)
    private List<String> parms;

    @Override
    protected void setDefaults() {
    }

    @Override
    protected boolean validateParms() throws IOException {
        return true;
    }

    @Override
    protected void runCommand() throws Exception {
        for (String parm : parms)
            log.info("Parm is \"{}\".", parm);
    }

}
