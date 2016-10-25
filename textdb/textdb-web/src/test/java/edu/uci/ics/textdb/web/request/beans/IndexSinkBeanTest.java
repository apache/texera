package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.IndexSinkBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the IndexSink operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class IndexSinkBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final IndexSinkBean indexSinkBean = new IndexSinkBean("operator1", "IndexSink", "attributes", "10", "100",
                "indexpath", "indexname");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"IndexSink\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"index_path\": \"indexpath\",\n" +
                "    \"index_name\": \"indexname\"\n" +
                "}";
        IndexSinkBean deserializedObject = MAPPER.readValue(jsonString, IndexSinkBean.class);
        assertEquals(indexSinkBean.equals(deserializedObject), true);
    }
}
