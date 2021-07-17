package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.io.StringReader;
import java.util.*;

import static edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc.partialAggregateSchema;


/**
 * Calculate word count and output count of each word.
 *
 * @author Mingji Han, Xiaozhen Liu
 */
public class WordCloudOpPartialExec implements OperatorExecutor {
    private final String textColumn;
    private Analyzer luceneAnalyzer;
    private List<String> textList;

    // for incremental computation
    public static final int UPDATE_INTERVAL_MS = 500;
    private long lastUpdatedTime = 0;

    public WordCloudOpPartialExec(String textColumn) {
        this.textColumn = textColumn;
    }

    private Analyzer getLuceneAnalyzer() {
        if (this.luceneAnalyzer == null) {
            this.luceneAnalyzer = new EnglishAnalyzer();
        }
        return this.luceneAnalyzer;
    }

    private static List<Tuple> calculateWordCount(List<String> texts, Analyzer luceneAnalyzer) throws Exception {
        HashMap<String, Integer> termFreqMap = new HashMap<>();

        for (String text : texts) {
            TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(text));
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);

            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                int charStart = offsetAttribute.startOffset();
                int charEnd = offsetAttribute.endOffset();
                String termStr = text.substring(charStart, charEnd).toLowerCase();
                if (!EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(termStr))
                    termFreqMap.put(termStr, termFreqMap.get(termStr) == null ? 1 : termFreqMap.get(termStr) + 1);
            }
            tokenStream.close();
        }
        List<Tuple> termFreqTuples = new ArrayList<>();

        for (Map.Entry<String, Integer> e : termFreqMap.entrySet()) {
            termFreqTuples.add(Tuple.newBuilder(partialAggregateSchema).addSequentially(new Object[]{e.getKey(), e.getValue()}).build());
        }
        return termFreqTuples;
    }

    @Override
    public void open() {
        textList = new ArrayList<>();
    }

    @Override
    public void close() {

    }

    @Override
    public String getParam(String query) {
        return null;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            textList.add(tuple.left().get().getField(textColumn).toString());
            boolean condition = System.currentTimeMillis() - lastUpdatedTime > UPDATE_INTERVAL_MS;
            if (condition) {
                lastUpdatedTime = System.currentTimeMillis();
                return computeResultIteratorForOneBatch();
            } else {
                return JavaConverters.asScalaIterator(Collections.emptyIterator());
            }
        } else { // input exhausted
            lastUpdatedTime = System.currentTimeMillis();
            return computeResultIteratorForOneBatch();
        }
    }

    public Iterator<Tuple> computeResultIteratorForOneBatch() {
        try {
            Iterator<Tuple> resultIterator = JavaConverters.asScalaIterator(
                    calculateWordCount(textList, getLuceneAnalyzer()).iterator());
            textList.clear();
            return resultIterator;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
