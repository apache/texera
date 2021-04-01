//package edu.uci.ics.texera.workflow.operators.source.scan.json;
//
//import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
//import edu.uci.ics.texera.workflow.common.tuple.Tuple;
//import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
//import scala.collection.Iterator;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//
//public class JSONLScanSourceOpExec implements SourceOperatorExecutor {
//
//    private final String localPath;
//    private final Schema schema;
//    private BufferedReader reader;
//    private String buffer;
//
//    JSONLScanSourceOpExec(String localPath, Schema schema) {
//        this.localPath = localPath;
//        this.schema = schema;
//
//    }
//
//    @Override
//    public Iterator<Tuple> produceTexeraTuple() {
//        return new Iterator<Tuple>() {
//
//            @Override
//            public boolean hasNext() {
//                try {
//                    return reader.ready();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public Tuple next() {
//                try {
//
//                    // obtain String representation of each field
//                    // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
//                    String line = reader.readLine();
//
//
//                    for (String fieldName : schema.getAttributeNames()) {
//
//                    }
//
//                    return Tuple.newBuilder().build();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//    }
//
//
//    @Override
//    public void open() {
//        try {
//
//
//            this.reader = new BufferedReader(new FileReader(this.localPath));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public void close() {
//        try {
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//    }
//
//}
