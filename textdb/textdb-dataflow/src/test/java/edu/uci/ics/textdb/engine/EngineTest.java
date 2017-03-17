package edu.uci.ics.textdb.engine;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;

public class EngineTest {

    private Engine engine;
    private Plan plan;
    private ISink sink;

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
        Mockito.when(plan.getRoot()).thenReturn(sink);
        engine.evaluate(plan);
        // Verify that open(), processTuples() and close() methods are called on
        // the Sink object
        Mockito.verify(sink).open();
        Mockito.verify(sink).processTuples();
        Mockito.verify(sink).close();
    }

    @Test
    public void testSingleton() {
        Engine engine2 = Engine.getEngine();
        Assert.assertSame(engine2, engine);
    }
}
