package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.constants.DataConstants;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the DictionaryMatcher operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class DictionaryMatcherBeanTest {

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();
    private DictionaryMatcherBean dictionaryMatcherBean;
    private Object deserializedObject;

    @Test
    public void testDeserialization() throws IOException{
        dictionaryMatcherBean = new DictionaryMatcherBean("operator1", "DictionaryMatcher",
                "attributes", "10", "100", "dictionary", "PHRASE_INDEXBASED");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"DictionaryMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"dictionary\": \"dictionary\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "}";
        deserializedObject = MAPPER.readValue(jsonString, DictionaryMatcherBean.class);
        assertEquals(dictionaryMatcherBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException{
        dictionaryMatcherBean = new DictionaryMatcherBean("operator1", "DictionaryMatcher",
                "attributes", "10", "100", "dictionary", "PHRASE_INDEXBASED");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"DictionaryMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"dictionary\": \"dictionary2\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "}";
        deserializedObject = MAPPER.readValue(jsonString, DictionaryMatcherBean.class);
        assertEquals(dictionaryMatcherBean.equals(deserializedObject), false);
    }
}