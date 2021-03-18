package org.theseed.p3api.common;

import java.util.Arrays;

import org.theseed.utils.BaseProcessor;

/**
 * This application performs various useful PATRIC API tasks.
 *
 * subfams		count the protein families for each role in a subsystem
 * roles		count the number of times each role occurs singly in a prokaryote
 * subcheck		validate the subsystems in a GTO against the subsystems in PATRIC
 * famCounts	count the protein families in genomes in a directory
 * roleCounts	count potentially-universal roles in a set of PATRIC genomes
 * simple		echo parameters (for testing)
 * fixMaster	add SSU rRNA information to a master genome directory
 *
 */
public class App
{
    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        switch (command) {
        case "subcheck" :
            processor = new SubsystemCheckProcessor();
            break;
        case "subfams" :
            processor = new SubFamilyProcessor();
            break;
        case "famCounts" :
            processor = new FamilyCountProcessor();
            break;
        case "roleCounts" :
            processor = new RoleCountProcessor();
            break;
        case "simple" :
            processor = new SimpleProcessor();
            break;
        case "fixMaster" :
            processor = new FixMasterProcessor();
            break;
        default :
            throw new RuntimeException("Invalid command " + command + ".");
        }
        boolean ok = processor.parseCommand(newArgs);
        if (ok) {
            processor.run();
        }
    }
}
