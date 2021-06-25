package edu.uci.ics.texera.web.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.join.JoinDistancePredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import edu.uci.ics.texera.web.TexeraWebApplication;
import edu.uci.ics.texera.web.TexeraWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class QueryPlanResourceTest {
    
    @ClassRule
    public static final DropwizardAppRule<TexeraWebConfiguration> RULE =
            new DropwizardAppRule<>(TexeraWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));
    
    public static String queryPlanEndpoint = "http://localhost:%d/api/queryplan/execute";
    
    public static final String TEST_TABLE = "query_plan_test_table";
    
    public static final Schema TEST_SCHEMA = new Schema(
            new Attribute("city", AttributeType.STRING), new Attribute("location", AttributeType.STRING),
            new Attribute("content", AttributeType.TEXT));
    
    @BeforeClass
    public static void setUp() throws Exception {
        RelationManager.getInstance().createTable(TEST_TABLE, Utils.getDefaultIndexDirectory().resolve(TEST_TABLE),
                TEST_SCHEMA, "standard");
    }
    
    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager.getInstance().deleteTable(TEST_TABLE);
    }
    
    public static KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(
            "irvine",
            Arrays.asList("city", "location", "content"),
            LuceneAnalyzerConstants.standardAnalyzerString(),
            KeywordMatchingType.PHRASE_INDEXBASED,
            TEST_TABLE,
            "keywordSourceResults");
    public static String KEYWORD_SOURCE_ID = "keyword source";
    
    public static RegexPredicate regexPredicate = new RegexPredicate(
            "ca(lifornia)?",
            Arrays.asList("location", "content"),
            "regexSourceResults");
    public static String REGEX_ID = "regex";
    
    public static JoinDistancePredicate joinDistancePredicate = new JoinDistancePredicate(
            "content",
            "content",
            100);
    public static String JOIN_DISTANCE_ID = "join distance";

    public static TupleSinkPredicate tupleSinkPredicate = new TupleSinkPredicate();
    public static String TUPLE_SINK_ID = "tuple sink";
    
    static {
        keywordSourcePredicate.setID(KEYWORD_SOURCE_ID);
        regexPredicate.setID(REGEX_ID);
        tupleSinkPredicate.setID(TUPLE_SINK_ID);
    }
    
    /*
     * It generates a valid logical plan as follows.
     *
     * KeywordSource --> RegexMatcher --> TupleSink
     *
     */
    public static LogicalPlan getLogicalPlan1() throws TexeraException {
        LogicalPlan logicalPlan = new LogicalPlan();

        logicalPlan.addOperator(keywordSourcePredicate);
        logicalPlan.addOperator(regexPredicate);
        logicalPlan.addOperator(tupleSinkPredicate);
        logicalPlan.addLink(new OperatorLink(KEYWORD_SOURCE_ID, REGEX_ID));
        logicalPlan.addLink(new OperatorLink(REGEX_ID, TUPLE_SINK_ID));
        return logicalPlan;
    }
    

    /**
     * Tests the query plan execution endpoint.
     */
    @Test
    public void checkSampleEndpoint() throws Exception {
        Client client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
        client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
        client.property(ClientProperties.READ_TIMEOUT,    5000);
        Response response = client.target(
                String.format(queryPlanEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(
                        new ObjectMapper().writeValueAsString(getLogicalPlan1()), 
                        MediaType.APPLICATION_JSON));
        
        assertThat(response.getStatus()).isEqualTo(200);
        

    }
}
