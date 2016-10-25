package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.LinkBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the Links between operators
 * Created by kishorenarendran on 10/20/16.
 */
public class LinkBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final LinkBean linkBean = new LinkBean("operator1", "operator2");
        String jsonString = "{\n" +
                "    \"from\": \"operator1\",\n" +
                "    \"to\": \"operator2\"\n" +
                "}";
        LinkBean deserializedObject = MAPPER.readValue(jsonString, LinkBean.class);
        assertEquals(linkBean.getToOperatorID().equals(deserializedObject.getToOperatorID()), true);
        assertEquals(linkBean.getFromOperatorID().equals(deserializedObject.getFromOperatorID()), true);
    }
}
