package edu.uci.ics.texera.dataflow.planstore;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.planstore.PlanStore;
import edu.uci.ics.texera.dataflow.planstore.PlanStoreConstants;
import edu.uci.ics.texera.storage.DataReader;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Adrian Seungjin Lee
 * @author Kishore Narendran
 */
public class PlanStoreTest {
    private static PlanStore planStore;

    // Data members that store Sample Logical Plan JSON Strings
    private static final String logicalPlanJson1 = "{\n" +
            "    \"operators\": [{\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"operator_type\": \"DictionarySource\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"data_source\": \"query_plan_resource_test_table\",\n" +
            "        \"dictionary\": \"dict1\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
            "    }, {\n" +
            "\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"operator_type\": \"TupleStreamSink\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\"\n" +
            "    }],\n" +
            "    \"links\": [{\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"    \n" +
            "    }]\n" +
            "}";

    private static final String logicalPlanJson2 = "{\n" +
            "    \"operators\": [{\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"operator_type\": \"KeywordSource\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"data_source\": \"query_plan_resource_test_table\",\n" +
            "        \"dictionary\": \"zika\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
            "    }, {\n" +
            "\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"operator_type\": \"TupleStreamSink\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\"\n" +
            "    }],\n" +
            "    \"links\": [{\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"    \n" +
            "    }]\n" +
            "}";


    /**
     * This function creates a PlanStore before every test
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        planStore = PlanStore.getInstance();
        planStore.createPlanStore();
    }

    /**
     * This method destroys the plan store after every test
     * @throws Exception
     */
    @After
    public void cleanUp() throws Exception {
        planStore.destroyPlanStore();
    }

    /**
     * This is a helper function that checks whether the plan corresponding to the plan name corresponds to the
     * logical plan JSON string that is fed to this function
     * @param planName - Name of the plan name to check with
     * @param logicalPlanJson - Expected LogicalPlan JSON string
     * @throws TexeraException
     */
    public static void assertCorrectPlanExists(String planName, String logicalPlanJson) throws TexeraException {
        Tuple res = planStore.getPlan(planName);

        Assert.assertNotNull(res);
        
        try {
            String returnedPlan = res.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(logicalPlanJson, JsonNode.class);
            JsonNode returnedJsonNode = objectMapper.readValue(returnedPlan, JsonNode.class);

            Assert.assertEquals(jsonNode, returnedJsonNode);
        } catch (IOException e) {
            throw new StorageException(e);
        }    
    }


    /**
     * This function checks whether the two given logical plan JSON strings are equivalent
     * @param plan1 - The first Logical Plan JSON
     * @param plan2 - The second Logical Plan JSON
     */
    public static void assertPlanEquivalence(String plan1, String plan2) {
        Assert.assertNotNull(plan1);
        Assert.assertNotNull(plan2);
        
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode1 = objectMapper.readValue(plan1, JsonNode.class);
            JsonNode jsonNode2 = objectMapper.readValue(plan2, JsonNode.class);

            Assert.assertEquals(jsonNode1, jsonNode2);
        } catch (IOException e) {
            throw new TexeraException(e);
        }
    }

    @Test
    public void testAddPlan() throws TexeraException {
        String planName = "plan";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);

        assertCorrectPlanExists(planName, logicalPlanJson1);
    }

    @Test
    public void testUpdatePlan() throws TexeraException {
        String planName1 = "plan1";

        planStore.addPlan(planName1, "basic dictionary source plan", logicalPlanJson1);

        planStore.updatePlan(planName1, logicalPlanJson2);

        assertCorrectPlanExists(planName1, logicalPlanJson2);
    }

    @Test
    public void testDeletePlan() throws TexeraException {
        String planName1 = "plan1";
        String planName2 = "plan2";

        planStore.addPlan(planName1, "basic dictionary source plan", logicalPlanJson1);
        planStore.addPlan(planName2, "basic keyword source plan", logicalPlanJson2);

        planStore.deletePlan(planName1);

        Tuple returnedPlan = planStore.getPlan(planName1);
        Assert.assertNull(returnedPlan);
    }

    @Test
    public void testPlanIterator() throws TexeraException {
        List<String> validPlans = new ArrayList<>();
        validPlans.add(logicalPlanJson1);
        validPlans.add(logicalPlanJson2);

        List<String> expectedPlans = new ArrayList<>();

        String planNamePrefix = "plan_";

        for (int i = 0; i < 100; i++) {
            String plan = validPlans.get(i % 2);
            expectedPlans.add(plan);
            planStore.addPlan(planNamePrefix + i, "basic plan " + i, plan);
        }


        DataReader reader = planStore.getPlanIterator();
        reader.open();

        Tuple tuple;
        String[] returnedPlans = new String[expectedPlans.size()];

        while ((tuple = reader.getNextTuple()) != null) {
            String planName = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
            int planIdx = Integer.parseInt(planName.split("_")[1]);
            String logicalPlanJson = tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString();
            returnedPlans[planIdx] = logicalPlanJson;
        }
        reader.close();

        for(int i = 0; i < expectedPlans.size(); i++) {
            assertPlanEquivalence(expectedPlans.get(i), returnedPlans[i]);
        }
    }

    @Test(expected = TexeraException.class)
    public void testAddPlanWithInvalidName() throws TexeraException {
        String planName = "plan/regex";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);
    }

    @Test(expected = TexeraException.class)
    public void testAddPlanWithEmptyName() throws TexeraException {
        String planName = "";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);
    }

    @Test(expected = TexeraException.class)
    public void testAddMultiplePlansWithSameName() throws TexeraException {
        String planName = "plan";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);
        planStore.addPlan(planName, "basic keyword source plan", logicalPlanJson2);
    }

    @Test
    public void testDeleteNotExistingPlan() throws TexeraException {
        String planName = "plan";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);

        planStore.deletePlan(planName + planName);

        assertCorrectPlanExists(planName, logicalPlanJson1);
    }

    @Test
    public void testUpdateNotExistingPlan() throws TexeraException {
        String planName = "plan";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);

        planStore.updatePlan(planName + planName, "basic dictionary source plan", logicalPlanJson1);

        assertCorrectPlanExists(planName, logicalPlanJson1);
    }

    @Test
    public void testNotDeletePlanBySubstring() throws TexeraException {
        String planName = "plan_sub";

        planStore.addPlan(planName, "basic dictionary source plan", logicalPlanJson1);

        planStore.deletePlan(planName.substring(0, 4));

        assertCorrectPlanExists(planName, logicalPlanJson1);
    }

}
