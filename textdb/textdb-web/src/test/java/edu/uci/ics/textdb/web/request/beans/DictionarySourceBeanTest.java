package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.web.request.beans.DictionarySourceBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the DictionarySource operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class DictionarySourceBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final DictionarySourceBean dictionarySourceBean = new DictionarySourceBean("operator1", "DictionarySource",
                "attributes", "10", "100", "dictionary", DataConstants.KeywordMatchingType.PHRASE_INDEXBASED,
                "datasource");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"DictionarySource\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"dictionary\": \"dictionary\",\n" +
                "    \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
                "    \"data_source\": \"datasource\"\n" +
                "}";
        DictionarySourceBean deserializedObject = MAPPER.readValue(jsonString, DictionarySourceBean.class);
        assertEquals(dictionarySourceBean.equals(deserializedObject), true);
    }
}
