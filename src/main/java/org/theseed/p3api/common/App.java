package org.theseed.p3api.common;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;

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
 * clean		remove obsolete genomes from a master genome directory
 * copy			copy genomes to a genome master directory
 * modulons		hook up the various modulon types for E coli
 * rnaCheck		verify SSU rRNA sequences against the SILVA database
 * rnaStats		compute statistics on SSU rRNA lengths
 * dnaDist		compute a the maximum distance between DNA FASTA sequences
 * bFinder		return the b-numbers for a send of gene names
 * binCheck		remove bad genomes from a binning reference genome FASTA
 * hammerX		check the misses from a hammer run against a distance file
 * zipCheck		validate a master directory with the ZipException bug
 * essential	determine which features in a list are essential
 * rnaFix2		fix up RNA Seq file names
 * unfixed		find unfixed chromosomes
 * rnaRestrain	build RNA Seq strain fixup data file
 * ssuFix		fix bad SSUs in the PATRIC master directory
 * qualCheck	compute the mean quality of a Fastq directory sample group
 * fastaG		update a genome from a FASTA file
 * hammerFix	convert hammer strengths to neighborhood-based
 * groupTest	generate a test dataset for the RNA Seq group file formatter
 * hammerComp	compare hammer counts to distances
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
        case "copy" :
            processor = new CopyMasterProcessor();
            break;
        case "clean" :
            processor = new CleanProcessor();
            break;
        case "modulons" :
            processor = new ModulonProcessor();
            break;
        case "rnaCheck" :
            processor = new RnaCheckProcessor();
            break;
        case "rnaStats" :
            processor = new RnaStatsProcessor();
            break;
        case "dnaDist" :
            processor = new DnaDistProcessor();
            break;
        case "bFinder" :
            processor = new BFinderProcessor();
            break;
        case "binCheck" :
            processor = new BinCheckProcessor();
            break;
        case "hammerX" :
            processor = new HammerTestProcessor();
            break;
        case "zipCheck" :
            processor = new ZipCheckProcessor();
            break;
        case "hammerCheck" :
            processor = new HammerCheckProcessor();
            break;
        case "essential" :
            processor = new EssentialProcessor();
            break;
        case "rnaFix" :
            processor = new RnaFixProcessor();
            break;
        case "fixConvert" :
            processor = new FixConvertProcessor();
            break;
        case "rnaFix2" :
            processor = new RnaFixProcessor2();
            break;
        case "rnaRestrain" :
            processor = new ReStrainMapProcessor();
            break;
        case "ssuFix" :
            processor = new SsuFixProcessor();
            break;
        case "qualCheck" :
            processor = new QualCheckProcessor();
            break;
        case "fastaG" :
            processor = new FastaGenomeProcessor();
            break;
        case "hammerFix" :
            processor = new HammerFixProcessor();
            break;
        case "groupTest" :
            processor = new GroupTestProcessor();
            break;
        case "hammerComp" :
            processor = new HammerCompareProcessor();
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
