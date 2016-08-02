/**
 * 
 */
package edu.uci.ics.textdb.common.constants;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

/**
 * @author sandeepreddy602
 * @author Zuozhi Wang (zuozhi)
 *
 */
public class DataConstants {
    public static final String INDEX_DIR = "../index";
    public static final String SCAN_QUERY = "*:*";
    public static final int MAX_RESULTS = 100;
    
    /**
     * KeywordMatchingType: the type of keyword matching to perform. <br>
     * Currently we have 3 types of keyword matching: <br>
     * 
     * SUBSTRING_SCANBASED: <br>
     * Performs simple substring matching of the query. 
     * SubString matching is case insensitive.
     * Source tuples are provided by ScanSourceOperator. <br>
     * 
     * CONJUNCTION_INDEXBASED: <br>
     * Performs search of conjunction of query tokens. 
     * The query is tokenized into keywords, with each token treated as a separate keyword. 
     * The order of tokens doesn't matter in the source tuple. <br>
     * 
     * For example: <br>
     * query "book appointment" <br>
     * matches: "book appointment with the doctor" <br>
     * also matches: "an appointment to pick up a book" <br>
     * <br>
     * 
     * 
     * PHRASE_INDEXBASED: <br>
     * Performs a phrase search.
     * The query is tokenized into keywords, with stopwords treated as placeholders.
     * The order of tokens matters in the source tuple. A stopword matches an arbitary token. <br>
     * 
     * For example: <br>
     * query "book appointment" <br>
     * matches: "book appointment with the doctor" <br>
     * doesn't match: "an appointment to pick up book" <br>
     * 
     * Example of stopword as placeholders: <br>
     * query "nice a a person":
     * matches "nice and beautiful person" <br>
     * matches "nice gentle honest person" <br>
     * doesn't match "nice person" <br>
     * doesn't match "nice gentle person" <br>
     * <br>
     * 
     * Default list of stopwords:
     * in org.apache.lucene.analysis.standard.StandardAnalyzer: <br>
     * StandardAnalyzer.STOP_WORDS_SET 
     * which includes:
     * 
     * but, be, with, such, then, for, no, will, not, are, and,
     * their, if, this, on, into, a, or, there, in, that, they, 
     * was, is, it, an, the, as, at, these, by, to, of
     * 
     */
    public static enum KeywordMatchingType {
        SUBSTRING_SCANBASED, 
        
        CONJUNCTION_INDEXBASED, 
        
        PHRASE_INDEXBASED
    };
    
    /**
     * @return a trigram analyzer that is used by RegexMatcher
     * @throws IOException
     */
    public static Analyzer getTrigramAnalyzer() throws IOException{
		return CustomAnalyzer.builder()
			.withTokenizer(NGramTokenizerFactory.class, new String[]{"minGramSize", "3", "maxGramSize", "3"})
			.addTokenFilter(LowerCaseFilterFactory.class)
			.build();
    }
      
}
