package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
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
import java.util.stream.Collectors;

/**
 * Merge word count maps into a single map (termFreqMap), calculate the size of each token based on its count, and
 * output as tuples of (word, count).
 *
 * @author Mingji Han, Xiaozhen Liu
 */
public class WordCloudOpFinalExec implements OperatorExecutor {

    private HashMap<String, Integer> termFreqMap;
    private final int topN;
    private static final Schema resultSchema = Schema.newBuilder().add(
            new Attribute("word", AttributeType.STRING),
            new Attribute("count", AttributeType.INTEGER)
    ).build();


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

    @Override
    public String getParam(String query) {
        return null;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            String term = tuple.left().get().getString(0);
            int frequency = tuple.left().get().getInt(1);
            termFreqMap.put(term, termFreqMap.get(term) == null ? frequency : termFreqMap.get(term) + frequency);
            return JavaConverters.asScalaIterator(Iterators.emptyIterator());
        } else {

            List<Map.Entry<String, Integer>> topNWordFreqs = termFreqMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .limit(topN).collect(Collectors.toList());

            List<Tuple> termFreqTuples = new ArrayList<>();
            for (Map.Entry<String, Integer> e : topNWordFreqs) {
                termFreqTuples.add(Tuple.newBuilder().add(
                        resultSchema,
                        Arrays.asList(e.getKey(), e.getValue())
                ).build());
            }
            return JavaConverters.asScalaIterator(termFreqTuples.iterator());
        }
    }
}
