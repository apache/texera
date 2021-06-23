package edu.uci.ics.texera.perftest.runme;

import java.io.File;

import edu.uci.ics.texera.perftest.utils.PerfTestUtils;

/*
 * @author Hailey Pan
 * 
 * Run this class to write all necessary index for performance tests!
 * 
 * Passed in below arguments:
 * 	file folder path (where data set stored)
 * 	standard index folder path (where standard index stored)
 * 	trigram index folder path (where trigram index stored)
 * 
 * If above arguments are not passed in, default paths will be used (refer to PerfTestUtils.java)
 * If some of the arguments are not applicable, define them as empty string. 
 * 
 * */
public class WriteIndex {

    public static void main(String[] args) {
        if (args.length != 0) {
            PerfTestUtils.setFileFolder(args[0]);
            PerfTestUtils.setStandardIndexFolder(args[1]);
            PerfTestUtils.setTrigramIndexFolder(args[2]);
        }

        try {
            PerfTestUtils.deleteDirectory(new File(PerfTestUtils.standardIndexFolder));
            PerfTestUtils.deleteDirectory(new File(PerfTestUtils.trigramIndexFolder));
            
            PerfTestUtils.writeStandardAnalyzerIndices();
            PerfTestUtils.writeTrigramIndices();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
