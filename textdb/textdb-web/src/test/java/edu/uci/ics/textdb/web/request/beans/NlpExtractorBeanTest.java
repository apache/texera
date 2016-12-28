package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.web.request.beans.NlpExtractorBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the NLPExtractor operators' properties
 * Created by kishorenarendran on 11/09/16.
 */
public class NlpExtractorBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final NlpExtractorBean nlpExtractorBean = new NlpExtractorBean("operator1", "NlpExtractor", "attributes",
                "10", "100", "Noun");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"NlpExtractor\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"nlp_type\": \"Noun\"\n" +
                "}";
        NlpExtractorBean deserializedObject = MAPPER.readValue(jsonString, NlpExtractorBean.class);
        assertEquals(nlpExtractorBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        final NlpExtractorBean nlpExtractorBean = new NlpExtractorBean("operator1", "NlpExtractor", "attributes",
                "10", "100", "Noun");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator2\",\n" +
                "    \"operator_type\": \"NlpExtractor\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"nlp_type\": \"Verb\"\n" +
                "}";
        NlpExtractorBean deserializedObject = MAPPER.readValue(jsonString, NlpExtractorBean.class);
        assertEquals(nlpExtractorBean.equals(deserializedObject), false);
    }
}