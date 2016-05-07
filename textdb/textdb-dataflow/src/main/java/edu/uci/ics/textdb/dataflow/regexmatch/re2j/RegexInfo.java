package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

import java.util.ArrayList;
import java.util.List;

/*
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * RegexInfo for translating regex to trigrams
 * see https://swtch.com/~rsc/regexp/regexp4.html for details
 */
public class RegexInfo {
	boolean emptyable;
	List<String> exact;
	List<String> prefix;
	List<String> suffix;
	TrigramBooleanQuery match;
	
	public RegexInfo() {
		emptyable = true;
		exact = new ArrayList<String>();
		prefix = new ArrayList<String>();
		suffix = new ArrayList<String>();
		match = new TrigramBooleanQuery();
	}
	
}