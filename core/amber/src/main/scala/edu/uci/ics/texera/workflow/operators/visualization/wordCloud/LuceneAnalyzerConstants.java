package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.IOException;

/**
 * LuceneAnalyzerConstants contains helper functions specifically
 *   used when dealing with different Lucene analyzers.
 *
 * @author Zuozhi Wang
 *
 */
public class LuceneAnalyzerConstants {
    public static final String STANDARD_ANALYZER = "standard";

    public static final String CHINESE_ANALYZER = "chinese";

    public static String standardAnalyzerString() {
        return STANDARD_ANALYZER;
    }

    /**
     * Uses Lucene's built-in smart chinese analyzer
     */
    public static String chineseAnalyzerString() {
        return CHINESE_ANALYZER;
    }

    /**
     * Gets the lucene analyzer based on the string, currently analyzers supported are:
     *   "standard", same as calling standardAnalyzerString().
     *
     *   "n-gram", n represents the number of grams, for example, "3-gram",
     *     same as calling nGramAnalyzerString(3).
     * @throws Exception, if the luceneAnalyzerString is invalid
     */
    public static Analyzer getLuceneAnalyzer(String luceneAnalyzerString) throws Exception {
        if (luceneAnalyzerString.equals("standard")) {
            return LuceneAnalyzerConstants.getStandardAnalyzer();
        }
        else if (luceneAnalyzerString.endsWith("-gram")) {
            try {
                int gramNum = Integer.parseInt(
                        luceneAnalyzerString.substring(0, luceneAnalyzerString.indexOf('-')));
                return getNGramAnalyzer(gramNum);
            } catch (NumberFormatException e) {
                throw new Exception(luceneAnalyzerString + " is not a valid lucene analyzer");
            }
        } else if (luceneAnalyzerString.equals("chinese")) {
            return new SmartChineseAnalyzer();
        }
        throw new Exception(luceneAnalyzerString + " is not a valid lucene analyzer");
    }

    public static Analyzer getStandardAnalyzer() {
        return new StandardAnalyzer();
    }

    /**
     * @return a n-gram analyzer that tokenizes the text into grams of length n.
     */
    public static Analyzer getNGramAnalyzer(int gramNum) throws Exception {
        try {
            return CustomAnalyzer.builder()
                    .withTokenizer(NGramTokenizerFactory.class,
                            "minGramSize", Integer.toString(gramNum), "maxGramSize", Integer.toString(gramNum))
                    .addTokenFilter(LowerCaseFilterFactory.class).build();
        } catch (IOException e) {
            throw new Exception(e.getMessage(), e);
        }
    }
}
