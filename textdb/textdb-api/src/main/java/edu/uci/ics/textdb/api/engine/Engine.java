package edu.uci.ics.textdb.api.engine;

import edu.uci.ics.textdb.api.dataflow.ISink;

/**
 * Created by chenli on 5/11/16.
 */
public class Engine {

    private static volatile Engine singletonEngine = null;

    private Engine() {
    }

    public static Engine getEngine() {
        if (singletonEngine == null) {
            synchronized (Engine.class) {
                if (singletonEngine == null) {
                    singletonEngine = new Engine();
                }
            }
        }
        return singletonEngine;
    }

    public void evaluate(Plan plan) throws Exception {
        ISink root = plan.getRoot();
        root.open();
        root.processTuples();
        root.close();
    }

    ;
}
