package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * RegexInfo for translating regex to trigrams
 * @see https://swtch.com/~rsc/regexp/regexp4.html for details
 */
public class RegexInfo {
	boolean emptyable;
	List<String> exact = null;
	List<String> prefix = null;
	List<String> suffix = null;
	TrigramBooleanQuery match = null;
	
	public RegexInfo() {
		emptyable = true;
		exact = new ArrayList<String>();
		prefix = new ArrayList<String>();
		suffix = new ArrayList<String>();
		match = new TrigramBooleanQuery();
	}
	
	
	public static RegexInfo matchNone() {
		RegexInfo info = new RegexInfo();
		info.match.operator = TrigramBooleanQuery.NONE;
		return info;
	}
	
	public static RegexInfo matchAll() {
		RegexInfo info = new RegexInfo();
		info.match.operator = TrigramBooleanQuery.ALL;
		return info;
	}
	
	
}