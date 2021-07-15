package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.ProgressiveUtils;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.*;
import java.util.stream.Collectors;

import static edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc.finalInsertRetractSchema;
import static edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc.partialAggregateSchema;

/**
 * Merge word count maps into a single map (termFreqMap), calculate the size of each token based on its count, and
 * output as tuples of (word, count).
 *
 * @author Mingji Han, Xiaozhen Liu
 */
public class WordCloudOpFinalExec implements OperatorExecutor {

    private List<Tuple> prevWordCloudTuples = new ArrayList<>();

    private HashMap<String, Integer> termFreqMap;
    private final int topN;

    public static final int UPDATE_INTERVAL_MS = 500;
    private long lastUpdatedTime = 0;
    private long counterSinceLastUpdate = 0;

    public WordCloudOpFinalExec(int topN) {
        this.topN = topN;
    }

    @Override
    public void open() {
        this.termFreqMap = new HashMap<>();
    }

    @Override
    public void close() {
        termFreqMap = null;
    }

    public List<Tuple> normalizeWordCloudTuples() {
        List<Map.Entry<String, Integer>> topNWordFreqs = termFreqMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(topN).collect(Collectors.toList());

        List<Tuple> termFreqTuples = new ArrayList<>();
        for (Map.Entry<String, Integer> e : topNWordFreqs) {
            termFreqTuples.add(Tuple.newBuilder(partialAggregateSchema)
                    .addSequentially(new Object[]{e.getKey(), e.getValue()})
                    .build()
            );
        }
        return termFreqTuples;
    }

    public List<Tuple> calculateResults(List<Tuple> normalizedWordCloudTuples) {
        List<Tuple> retractions = new ArrayList<>(prevWordCloudTuples);
        List<Tuple> insertions = new ArrayList<>(normalizedWordCloudTuples);

        retractions.removeAll(normalizedWordCloudTuples);
        insertions.removeAll(prevWordCloudTuples);

        List<Tuple> results = new ArrayList<>();
        retractions.forEach(tuple -> results.add(ProgressiveUtils.addRetractionFlag(tuple, finalInsertRetractSchema)));
        insertions.forEach(tuple -> results.add(ProgressiveUtils.addInsertionFlag(tuple, finalInsertRetractSchema)));
        return results;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            String term = tuple.left().get().getString(0);
            int frequency = tuple.left().get().getInt(1);
            termFreqMap.put(term, termFreqMap.get(term) == null ? frequency : termFreqMap.get(term) + frequency);

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
                return JavaConverters.asScalaIterator(Collections.emptyIterator());
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
                return JavaConverters.asScalaIterator(Collections.emptyIterator());
            }
        }
    }

}
