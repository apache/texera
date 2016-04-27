package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import java.io.File;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.common.constants.LuceneConstants;

/* 
 * This example shows that lucene can process manually translated boolean expressions.   
 */

public class LuceneNgramQueryExample {
	public String dataFileName;
	
	private Analyzer analyzer;
	private IndexSearcher searcher;
	private IndexWriter indexWriter;
	
	public LuceneNgramQueryExample(String dfn, int minNgramSsize, int maxNgramSize) throws Exception {
		dataFileName = dfn;

		analyzer = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Tokenizer source = new NGramTokenizer(minNgramSsize, maxNgramSize);
				return new TokenStreamComponents(source);
			}
		};
		Directory indexDir = FSDirectory.open(Paths.get(LuceneConstants.INDEX_DIR));
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		indexWriter = new IndexWriter(indexDir, config);
	}
	
	public void buildNGramIndex() throws Exception {
		indexWriter.deleteAll();
		
		Scanner fileScanner = new Scanner(new File(dataFileName));
		Integer lineNum = 1;
		while (fileScanner.hasNextLine()) {
			Document doc = new Document();
			FieldType type = getFieldType();
			Field dataField = new Field("data", fileScanner.nextLine(), type);
			Field idField = new Field("id", lineNum.toString(), type);
			doc.add(dataField);
			doc.add(idField);
			indexWriter.addDocument(doc);
			lineNum += 1;
		}
		fileScanner.close();
		indexWriter.close();
	}
	
	private FieldType getFieldType() {
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS);
		type.setStored(true);
		type.setStoreTermVectors(true);
		return type;
	}
	
	public TopDocs search(String queryText, int numOfTopHit) throws Exception{
		searcher = new IndexSearcher(
				DirectoryReader.open(FSDirectory.open(Paths.get(LuceneConstants.INDEX_DIR))));
		
        QueryParser parser = new QueryParser("data", analyzer);
		Query query = parser.parse(queryText);
		TopDocs res = searcher.search(query, numOfTopHit);

		return res;

	}
	
	public IndexSearcher getSearcher() {
		return searcher;
	}
}
