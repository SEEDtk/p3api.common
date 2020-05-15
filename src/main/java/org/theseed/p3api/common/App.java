package org.theseed.p3api.common;

import java.util.Arrays;

import org.theseed.utils.ICommand;

/**
 * This application performs various useful PATRIC API tasks.
 *
 * subfams	count the protein families for each role in a subsystem
 *
 */
public class App
{
    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        ICommand processor;
        switch (command) {
        case "subfams" :
            processor = new SubFamilyProcessor();
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
