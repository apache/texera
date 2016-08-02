package edu.uci.ics.textdb.perftest.runme;

/**
 * @author Hailey Pan
 * */

import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;

/*
 * Run this class to write all necessary index for performance tests!
 * */
public class WriteIndex {
	public static void main(String[] args) {

		try {
			PerfTestUtils.writeStandardAnalyzerIndices();
			PerfTestUtils.writeTrigramIndices();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
