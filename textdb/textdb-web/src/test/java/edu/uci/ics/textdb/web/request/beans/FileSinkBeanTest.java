package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.web.request.beans.DictionarySourceBean;
import edu.uci.ics.textdb.web.request.beans.FileSinkBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the FileSink operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class FileSinkBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final FileSinkBean fileSinkBean = new FileSinkBean("operator1", "FileSink", "attributes", "10", "100",
                "filepath");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"FileSink\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"file_path\": \"filepath\"\n" +
                "}";
        FileSinkBean deserializedObject = MAPPER.readValue(jsonString, FileSinkBean.class);
        assertEquals(fileSinkBean.equals(deserializedObject), true);
    }
}
