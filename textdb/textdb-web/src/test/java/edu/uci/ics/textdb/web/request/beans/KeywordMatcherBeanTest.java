package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.web.request.beans.FileSinkBean;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the KeywordMatcher operators' properties
 * Created by kishorenarendran on 11/09/16.
 */
public class KeywordMatcherBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final KeywordMatcherBean keywordMatcherBean = new KeywordMatcherBean("operator1", "KeywordMatcher",
                "attributes", "10", "100", "keyword1", "PHRASE_INDEXBASED");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"KeywordMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"keyword\": \"keyword1\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "}";
        KeywordMatcherBean deserializedObject = MAPPER.readValue(jsonString, KeywordMatcherBean.class);
        assertEquals(keywordMatcherBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        final KeywordMatcherBean keywordMatcherBean = new KeywordMatcherBean("operator1", "KeywordMatcher",
                "attributes", "10", "100", "keyword1", "PHRASE_INDEXBASED");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator2\",\n" +
                "    \"operator_type\": \"KeywordMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"keyword\": \"keyword2\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "}";
        KeywordMatcherBean deserializedObject = MAPPER.readValue(jsonString, KeywordMatcherBean.class);
        assertEquals(keywordMatcherBean.equals(deserializedObject), false);
    }
}