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
 * clean		remove obsolete genomes from a master genome directory
 * copy			copy genomes to a genome master directory
 * rnaCheck		verify SSU rRNA sequences against the SILVA database
 * rnaStats		compute statistics on SSU rRNA lengths
 * dnaDist		compute the maximum distance between DNA FASTA sequences
 * binCheck		remove bad genomes from a binning reference genome FASTA
 * hammerX		check the misses from a hammer run against a distance file
 * essential	determine which features in a list are essential
 * ssuFix		fix bad SSUs in the PATRIC master directory
 * qualCheck	compute the mean quality of a Fastq directory sample group
 * fastaG		update a genome from a FASTA file
 * hammerFix	convert hammer strengths to neighborhood-based
 * groupTest	generate a test dataset for the RNA Seq group file formatter
 * hammerComp	compare hammer counts to distances
 * md5Check		check a genome dump directory for MD5s in a protein list
 * vsynth		create synthetic viruses from other viruses in a FASTA
 * findBig		find the largest file of each type in a directory of directories
 * findAmr		find high-quality genomes in BV-BRC with AMR data
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
       case "copy" :
            processor = new CopyMasterProcessor();
            break;
        case "clean" :
            processor = new CleanProcessor();
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
        case "binCheck" :
            processor = new BinCheckProcessor();
            break;
        case "hammerX" :
            processor = new HammerTestProcessor();
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
        case "groupTest" :
            processor = new GroupTestProcessor();
            break;
        case "hammerComp" :
            processor = new HammerCompareProcessor();
            break;
        case "md5Check" :
            processor = new Md5CheckProcessor();
            break;
        case "vsynth" :
            processor = new VSynthProcessor();
            break;
        case "findBig" :
            processor = new FindBigFileProcessor();
            break;
        case "findAmr" :
            processor = new FindAmrGenomeProcessor();
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
