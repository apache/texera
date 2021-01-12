package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.texera.workflow.common.ProgressiveUtils;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.curator.shaded.com.google.common.collect.Iterators;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.*;

/**
 * Merge word count maps into a single map (termFreqMap), calculate the size of each token based on its count, and
 * output as tuples of (word, size).
 * @author Mingji Han, Xiaozhen Liu
 *
 */
public class WordCloudOpFinalExec implements OperatorExecutor {
    private static final int MAX_FONT_SIZE = 200;
    private static final int MIN_FONT_SIZE = 50;

    private List<Tuple> prevWordCloudTuples = new ArrayList<>();
    private HashMap<String, Integer> termFreqMap = new HashMap<>();

    private static final Schema resultSchema = Schema.newBuilder().add(
            new Attribute("word", AttributeType.STRING),
            new Attribute("size", AttributeType.INTEGER)
    ).build();

    public static final int UPDATE_INTERVAL_MS = 500;
    private long lastUpdatedTime = 0;
    private long counterSinceLastUpdate = 0;

    @Override
    public void open() {
        this.termFreqMap = new HashMap<>();
    }

    @Override
    public void close() {
        termFreqMap = null;
    }

    public List<Tuple> normalizeWordCloudTuples() {
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;

        for (Map.Entry<String, Integer> e : termFreqMap.entrySet()) {
            int frequency = e.getValue();
            minValue = Math.min(minValue, frequency);
            maxValue = Math.max(maxValue, frequency);
        }
        // normalize the font size for wordcloud js
        // https://github.com/timdream/wordcloud2.js/issues/53
        List<Tuple> termFreqTuples = new ArrayList<>();
        for (Map.Entry<String, Integer> e : termFreqMap.entrySet()) {
            termFreqTuples.add(Tuple.newBuilder().add(
                    resultSchema,
                    Arrays.asList(
                            e.getKey(),
                            (int) ((e.getValue() - minValue) / (maxValue - minValue) *
                                    (MAX_FONT_SIZE - MIN_FONT_SIZE) + MIN_FONT_SIZE)
                    )
            ).build());
        }
        return termFreqTuples;
    }

    public List<Tuple> calculateResults(List<Tuple> normalizedWordCloudTuples) {
        List<Tuple> retractions = new ArrayList<>(prevWordCloudTuples);
        List<Tuple> insertions = new ArrayList<>(normalizedWordCloudTuples);

        retractions.removeAll(normalizedWordCloudTuples);
        insertions.removeAll(prevWordCloudTuples);

        List<Tuple> results = new ArrayList<>();
        retractions.forEach(tuple -> results.add(ProgressiveUtils.addRetractionFlag(tuple)));
        insertions.forEach(tuple -> results.add(ProgressiveUtils.addInsertionFlag(tuple)));
        return results;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, int input) {
        if(tuple.isLeft()) {
            String term = tuple.left().get().getString(0);
            int frequency = tuple.left().get().getInt(1);
            termFreqMap.put(term, termFreqMap.get(term)==null ? frequency : termFreqMap.get(term) + frequency);

            counterSinceLastUpdate++;
            boolean condition = System.currentTimeMillis() - lastUpdatedTime > UPDATE_INTERVAL_MS;
            if (condition) {
                counterSinceLastUpdate = 0;
                lastUpdatedTime = System.currentTimeMillis();

                List<Tuple> normalizedWordCloudTuples = normalizeWordCloudTuples();
                List<Tuple> results = calculateResults(normalizedWordCloudTuples);
                prevWordCloudTuples = normalizedWordCloudTuples;
                return JavaConverters.asScalaIterator(results.iterator());
            } else {
                return JavaConverters.asScalaIterator(Iterators.emptyIterator());
            }
        } else {
            if (counterSinceLastUpdate > 0) {
                lastUpdatedTime = System.currentTimeMillis();
                counterSinceLastUpdate = 0;

                List<Tuple> normalizedWordCloudTuples = normalizeWordCloudTuples();
                List<Tuple> results = calculateResults(normalizedWordCloudTuples);
                prevWordCloudTuples = normalizedWordCloudTuples;
                return JavaConverters.asScalaIterator(results.iterator());
            } else {
                return JavaConverters.asScalaIterator(Iterators.emptyIterator());
            }

        }
    }


}