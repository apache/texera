package edu.uci.ics.textdb.web.request;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Checks the deserialization of a logical query plan
 * Created by kishorenarendran on 10/21/16.
 */
public class QueryPlanRequestTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        // TODO - This JSON needs to be updated with a new JSON string corresponding to the changes in the Join oerator
        String jsonString = "{\n" +
                "   \"operators\":[\n" +
                "      {\n" +
                "         \"operator_id\":\"KeywordSource_0\",\n" +
                "         \"operator_type\":\"KeywordSource\",\n" +
                "         \"keyword\":\"zika\",\n" +
                "         \"data_source\":\"sample\",\n" +
                "         \"matching_type\":\"conjunction\",\n" +
                "         \"attributes\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"Projection_1\",\n" +
                "         \"operator_type\":\"Projection\",\n" +
                "         \"attributes\":\"_id, id, content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"RegexMatcher_2\",\n" +
                "         \"operator_type\":\"RegexMatcher\",\n" +
                "         \"regex\":\"((((0?[1-9])|(1[0-2]))(\\\\s|-|.|\\\\/)((0?[1-9])|([12][0-9])|(3[01]))(\\\\s|-|.|\\\\/)([0-9]{4}|[0-9]{2}))|(((0?[1-9])|([12][0-9])|(3[01])) ((jan(uary)?)|(feb(ruary)?)|(mar(ch)?)|(apr(il)?)|(may)|(june?)|(july?)|(aug(ust)?)|(sep(tember)?)|(oct(ober)?)|(nov(ember)?)|(dec(ember)?)) ([0-9]{4}|[0-9]{2})))\",\n" +
                "         \"attributes\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"RegexMatcher_3\",\n" +
                "         \"operator_type\":\"RegexMatcher\",\n" +
                "         \"regex\":\"\\\\b(A|a|(an)|(An)) .{1,40} ((woman)|(man))\\\\b\",\n" +
                "         \"attributes\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"NlpExtractor_5\",\n" +
                "         \"operator_type\":\"NlpExtractor\",\n" +
                "         \"nlp_type\":\"location\",\n" +
                "         \"attributes\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"Join_6\",\n" +
                "         \"operator_type\":\"Join\",\n" +
                "         \"predicate_type\":\"CharacterDistance\",\n" +
                "         \"threshold\":\"100\",\n" +
                "         \"inner_attribute\":\"content\",\n" +
                "         \"outer_attribute\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"Join_7\",\n" +
                "         \"operator_type\":\"Join\",\n" +
                "         \"predicate_type\":\"CharacterDistance\",\n" +
                "         \"threshold\":\"100\",\n" +
                "         \"inner_attribute\":\"content\",\n" +
                "         \"outer_attribute\":\"content\",\n" +
                "         \"limit\":\"100\",\n" +
                "         \"offset\":\"0\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"operator_id\":\"TupleStreamSink_8\",\n" +
                "         \"operator_type\":\"TupleStreamSink\",\n" +
                "         \"attributes\":\"first name, last name\",\n" +
                "         \"limit\":10,\n" +
                "         \"offset\":0\n" +
                "      }\n" +
                "   ],\n" +
                "   \"links\":[\n" +
                "      {\n" +
                "         \"from\":\"KeywordSource_0\",\n" +
                "         \"to\":\"Projection_1\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"Projection_1\",\n" +
                "         \"to\":\"RegexMatcher_2\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"Projection_1\",\n" +
                "         \"to\":\"RegexMatcher_3\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"Projection_1\",\n" +
                "         \"to\":\"NlpExtractor_5\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"RegexMatcher_2\",\n" +
                "         \"to\":\"Join_7\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"RegexMatcher_3\",\n" +
                "         \"to\":\"Join_7\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"NlpExtractor_5\",\n" +
                "         \"to\":\"Join_6\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"Join_7\",\n" +
                "         \"to\":\"Join_6\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"from\":\"Join_6\",\n" +
                "         \"to\":\"TupleStreamSink_8\"\n" +
                "      }\n" +
                "   ]\n" +
                "}";
        QueryPlanRequest queryPlanRequest = MAPPER.readValue(jsonString, QueryPlanRequest.class);
        assertEquals(queryPlanRequest.getOperatorBeans().size(), 8);
        assertEquals(queryPlanRequest.getOperatorLinkBeans().size(), 9);
    }

    @Test
    public void testInvalidOperatorTypeDeserialization() throws IOException {
        String jsonString = "{\n" +
                "    \"operators\": [{\n" +
                "        \"operator_id\": \"operator1\",\n" +
                "        \"operator_type\": \"SomeRandomOperatorType\",\n" +
                "        \"attributes\": \"attributes\",\n" +
                "        \"limit\": \"10\",\n" +
                "        \"offset\": \"100\",\n" +
                "        \"dictionary\": \"dict1\",\n" +
                "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "    }, {\n" +
                "\n" +
                "        \"operator_id\": \"operator2\",\n" +
                "        \"operator_type\": \"DictionaryMatcher\",\n" +
                "        \"attributes\": \"attributes\",\n" +
                "        \"limit\": \"10\",\n" +
                "        \"offset\": \"100\",\n" +
                "        \"dictionary\": \"dict2\",\n" +
                "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
                "    }],\n" +
                "    \"links\": [{\n" +
                "        \"from\": \"operator1\",\n" +
                "        \"to\": \"operator2\"    \n" +
                "    }]\n" +
                "}";
        boolean exceptionThrownFlag = false;
        try {
            MAPPER.readValue(jsonString, QueryPlanRequest.class);
        }
        catch(JsonMappingException e) {
            exceptionThrownFlag = true;
        }
        assertEquals(exceptionThrownFlag, true);
    }
}
