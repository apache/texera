package edu.uci.ics.textdb.dataflow.source;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.SampleTuple;

public class SampleDataStore {
	public SampleDataStore(){
		
	}
	
	public void storeData() throws Exception{
		IndexWriter indexWriter = null;
		try {
			Directory directory = FSDirectory.open(Paths.get(Constants.INDEX_DIR));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig conf = new IndexWriterConfig(analyzer );
			indexWriter = new IndexWriter(directory, conf);
			for(ITuple sampleTuple : Constants.SAMPLE_TUPLES){

				Document document = getDocument(sampleTuple);
				indexWriter.addDocument(document);
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}finally{
			if(indexWriter != null){
				indexWriter.close();
			}
		}
	}

	private Document getDocument(ITuple tuple) {
		SampleTuple sampleTuple = (SampleTuple) tuple;
		Document doc = new Document();
		
		IField fnField = sampleTuple.getField(Constants.FIRST_NAME);
		String fn = "";
		if(fnField != null && fnField instanceof StringField){
			fn = ((StringField) fnField).getValue();
		}
		
		IField lnField = sampleTuple.getField(Constants.LAST_NAME);
		String ln = "";
		if(lnField != null && lnField instanceof StringField){
			ln = ((StringField)lnField).getValue();
		}
		
		
		IndexableField fnLuceneField = new org.apache.lucene.document.StringField(
				Constants.FIRST_NAME, fn, Store.YES); 
		IndexableField lnLuceneField = new org.apache.lucene.document.StringField(
				Constants.LAST_NAME, ln, Store.YES); 
		doc.add(fnLuceneField);
		doc.add(lnLuceneField);
		return doc;
	}
}
