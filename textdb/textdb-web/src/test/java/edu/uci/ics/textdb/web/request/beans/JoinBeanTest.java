package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.JoinBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the JoinBean operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class JoinBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final JoinBean joinBean = new JoinBean("Join_01", "Join", null, null, "inner_attr_name", "outer_attr_name",
                "CharacterDistance", "10");
        String jsonString = "{\n" +
                "   \"operator_id\":\"Join_01\",\n" +
                "   \"operator_type\":\"Join\",\n" +
                "   \"inner_attribute\":\"inner_attr_name\",\n" +
                "   \"outer_attribute\":\"outer_attr_name\",\n" +
                "   \"predicate_type\":\"CharacterDistance\",\n" +
                "   \"threshold\":\"10\"\n" +
                "}";
        JoinBean deserializedObject = MAPPER.readValue(jsonString, JoinBean.class);
        assertEquals(joinBean.equals(deserializedObject), true);
    }

    @Test
    public void testInvalidDeserialization() throws IOException {
        final JoinBean joinBean = new JoinBean("Join_01", "Join", null, null, "inner_attr_names", "outer_attr_names",
                "CharacterDistance", "10");
        String jsonString = "{\n" +
                "   \"operator_id\":\"Join_01\",\n" +
                "   \"operator_type\":\"Join\",\n" +
                "   \"inner_attribute\":\"inner_attr_name\",\n" +
                "   \"outer_attribute\":\"outer_attr_name\",\n" +
                "   \"predicate_type\":\"CharacterDistance\",\n" +
                "   \"threshold\":\"10\"\n" +
                "}";
        JoinBean deserializedObject = MAPPER.readValue(jsonString, JoinBean.class);
        assertEquals(joinBean.equals(deserializedObject), false);
    }
}