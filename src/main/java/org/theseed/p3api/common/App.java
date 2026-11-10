package org.theseed.p3api.common;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;

/**
 * This application performs various useful PATRIC API tasks.
 *
 * subfams		count the protein families for each role in a subsystem
 * subcheck		validate the subsystems in a GTO against the subsystems in PATRIC
 * famCounts	count the protein families in genomes in a directory
 * simple		echo parameters (for testing)
 * clean		remove obsolete genomes from a master genome directory
 * rnaCheck		verify SSU rRNA sequences against the SILVA database
 * rnaStats		compute statistics on SSU rRNA lengths
 * dnaDist		compute the maximum distance between DNA FASTA sequences
 * binCheck		remove bad genomes from a binning reference genome FASTA
 * hammerX		check the misses from a hammer run against a distance file
 * essential	determine which features in a list are essential
 * qualCheck	compute the mean quality of a Fastq directory sample group
 * fastaG		update a genome from a FASTA file
 * hammerComp	compare hammer counts to distances
 * md5Check		check a genome dump directory for MD5s in a protein list
 * doneCheck	remove completed groups from a run list
 * findBig		find the largest file of each type in a directory of directories
 * mergeCol		merge a column from one tab-delimited file into a single-column file
 * combineJson	combine multiple JSON files into a single file
 * coreReport   look at the results of the core genome match command and produce a report
 * funReport    look at the results of the functional role match command and produce a report
 * fastaLen     compute the length of the sequences in a FASTA file
 * finderSamp   test finder-kmer representative sets
 * finderHist   create histogram bins from finder-kmer closeness values in one or more genome files
 * finderForm   compute formula coefficients from the results of finderSamp
 * finderTable  create a table of finderSamp results
 * finderReps   generate a representative set for a group of genomes using finder-kmer technology
 * amrGenera    analyze AMR data at the genus level
 * amrTests     generate genome groups for the AMR repgen test
 * randTests    generate random test groups for repgen tests
 * testClean    clean duplicates from AMR test files
 *
 */
public class App
{

    protected static final String[] COMMANDS = new String[] {
         "subfams", "count the protein families for each role in a subsystem",
 		 "subcheck", "validate the subsystems in a GTO against the subsystems in PATRIC",
 		 "famCounts", "count the protein families in genomes in a directory",
 		 "simple", "echo parameters (for testing)",
 		 "clean", "remove obsolete genomes from a master genome directory",
 		 "rnaCheck", "verify SSU rRNA sequences against the SILVA database",
 		 "rnaStats", "compute statistics on SSU rRNA lengths",
 		 "dnaDist", "compute the maximum distance between DNA FASTA sequences",
 		 "binCheck", "remove bad genomes from a binning reference genome FASTA",
 		 "hammerX", "check the misses from a hammer run against a distance file",
 		 "essential", "determine which features in a list are essential",
 		 "qualCheck", "compute the mean quality of a Fastq directory sample group",
 		 "fastaG", "update a genome from a FASTA file",
 		 "hammerComp", "compare hammer counts to distances",
 		 "md5Check", "check a genome dump directory for MD5s in a protein list",
 		 "doneCheck", "remove completed groups from a run list",
 		 "findBig", "find the largest file of each type in a directory of directories",
 		 "mergeCol", "merge a column from one tab-delimited file into a single-column file",
 		 "combineJson", "combine multiple JSON files into a single file",
 		 "coreReport", "look at the results of the core genome match command and produce a report",
 		 "funReport", "look at the results of the functional role match command and produce a report",
 		 "mergeCol", "merge a column from one tab-delimited file into a single-column file",
 		 "combineJson", "combine multiple JSON files into a single file",
 		 "fastaLen", "compute the length of the sequences in a FASTA file",
 		 "finderSamp", "create a finder kmer testing file for a set of genomes",
         "finderForm", "compute formula coefficients from the results of finderSamp",
         "finderTable", "create a table of finderSamp results",
         "amrGenera", "analyze AMR data at the genus level",
         "amrTests", "generate genome groups for the AMR repgen test",
         "randTests", "generate random test groups for repgen tests",
         "testClean", "clean duplicates from AMR test files"
    };

    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        switch (command) {
        case "subcheck" -> processor = new SubsystemCheckProcessor();
        case "subfams" -> processor = new SubFamilyProcessor();
        case "famCounts" -> processor = new FamilyCountProcessor();
        case "simple" -> processor = new SimpleProcessor();
        case "clean" -> processor = new CleanProcessor();
        case "rnaCheck" -> processor = new RnaCheckProcessor();
        case "rnaStats" -> processor = new RnaStatsProcessor();
        case "dnaDist" -> processor = new DnaDistProcessor();
        case "hammerX" -> processor = new HammerTestProcessor();
        case "hammerCheck" -> processor = new HammerCheckProcessor();
        case "essential" -> processor = new EssentialProcessor();
        case "rnaRestrain" -> processor = new ReStrainMapProcessor();
        case "qualCheck" -> processor = new QualCheckProcessor();
        case "fastaG" -> processor = new FastaGenomeProcessor();
        case "hammerComp" -> processor = new HammerCompareProcessor();
        case "md5Check" -> processor = new Md5CheckProcessor();
        case "doneCheck" -> processor = new DoneCheckProcessor();
        case "findBig" -> processor = new FindBigFileProcessor();
        case "amrTests" -> processor = new AmrTestsProcessor();
        case "mergeCol" -> processor = new MergeColumnProcessor();
        case "combineJson" -> processor = new CombineJsonProcessor();
        case "coreReport" -> processor = new CoreReportProcessor();
        case "funReport" -> processor = new FunReportProcessor();
        case "fastaLen" -> processor = new FastaLenProcessor();
        case "finderSamp" -> processor = new FinderSampProcessor();
        case "finderHist" -> processor = new FinderHistProcessor();
        case "finderForm" -> processor = new FinderFormProcessor();
        case "finderTable" -> processor = new FinderTableProcessor();
        case "amrGenera" -> processor = new AmrGeneraProcessor();
        case "randTests" -> processor = new RandomTestProcessor();
        case "testClean" -> processor = new TestCleanProcessor();
        case "-h", "--help" -> processor = null;
        default -> throw new RuntimeException("Invalid command " + command + ".");
        }
        if (processor == null)
            BaseProcessor.showCommands(COMMANDS);
        else {
            processor.parseCommand(newArgs);
            processor.run();
        }
    }
}
