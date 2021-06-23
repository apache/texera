package edu.uci.ics.texera.api.engine;

import java.util.HashMap;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.texera.api.dataflow.ISink;

public class EngineTest {

    private Engine engine;
    private Plan plan;
    private ISink sink;
    private HashMap<String, ISink> sinkMap;
    @Before
    public void setUp() {
        engine = Engine.getEngine();
        // mock the Plan object
        plan = Mockito.mock(Plan.class);
        // mock Sink object
        sink = Mockito.mock(ISink.class);
    }

    @Test
    public void testEvaluate() throws Exception {
        // set behavior for Plan Object.

        Mockito.when(plan.getSinkMap()).thenReturn(sinkMap);
        sinkMap = new HashMap<>();

        engine.evaluate(plan);
        // Verify that open(), processTuples() and close() methods are called on
        // the Sink object
        Mockito.verify(plan).getSinkMap();

    }

    @Test
    public void testSingleton() {
        Engine engine2 = Engine.getEngine();
        Assert.assertSame(engine2, engine);
    }
}
