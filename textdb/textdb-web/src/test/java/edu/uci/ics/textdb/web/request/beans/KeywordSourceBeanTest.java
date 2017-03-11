package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.web.request.beans.FileSinkBean;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import edu.uci.ics.textdb.web.request.beans.KeywordSourceBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the KeywordSource operators' properties
 * Created by kishorenarendran on 11/09/16.
 */
public class KeywordSourceBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final KeywordSourceBean keywordSourceBean = new KeywordSourceBean("operator1", "KeywordSource", "attributes",
                "10", "100", "keyword1", "PHRASE_INDEXBASED", "datasource");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"KeywordSource\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"keyword\": \"keyword1\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
                "    \"data_source\": \"datasource\"\n" +
                "}";
        KeywordSourceBean deserializedObject = MAPPER.readValue(jsonString, KeywordSourceBean.class);
        assertEquals(keywordSourceBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        final KeywordSourceBean keywordSourceBean = new KeywordSourceBean("operator1", "KeywordSource", "attributes",
                "10", "100", "keyword1", "PHRASE_INDEXBASED", "datasource");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator2\",\n" +
                "    \"operator_type\": \"KeywordSource\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"keyword\": \"keyword2\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
                "    \"data_source\": \"datasource1\"\n" +
                "}";
        KeywordSourceBean deserializedObject = MAPPER.readValue(jsonString, KeywordSourceBean.class);
        assertEquals(keywordSourceBean.equals(deserializedObject), false);
    }
}