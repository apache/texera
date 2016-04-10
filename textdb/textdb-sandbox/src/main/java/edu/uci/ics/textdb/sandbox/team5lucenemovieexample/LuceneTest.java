package edu.uci.ics.textdb.sandbox.team5lucenemovieexample;

import java.io.IOException;

public class LuceneTest {

	public static void main(String[] args) {
		Indexer id = new Indexer();
		Searcher s;
		try {
			id.rebuildIndexes();
			s = new Searcher();
			s.performSearch("mand", 10);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
