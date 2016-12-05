package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.plangen.LogicalPlanTest;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author sweetest
 */
public class PlanStoreTest {
    private static PlanStore planStore;

    @Before
    public void setUp() throws Exception {
        planStore = PlanStore.getInstance();
        planStore.createPlanStore();
    }

    @After
    public void cleanUp() throws Exception {
        planStore.destroyPlanStore();
    }

    public static void assertCorrectPlanExists(String planName, LogicalPlan logicalPlan) throws TextDBException {
        ITuple res = planStore.getPlan(planName);

        Assert.assertNotNull(res);

        String filePath = res.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
        LogicalPlan returnedPlan = planStore.readPlanObject(filePath);

        Assert.assertEquals(logicalPlan, returnedPlan);
    }

    @Test
    public void testAddPlan() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();
        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        assertCorrectPlanExists(planName, logicalPlan);
    }

    @Test
    public void testUpdatePlan() throws TextDBException {
        LogicalPlan logicalPlan1 = LogicalPlanTest.getLogicalPlan1();
        LogicalPlan logicalPlan2 = LogicalPlanTest.getLogicalPlan2();

        String planName1 = "plan1";

        planStore.addPlan(planName1, "basic regex plan", logicalPlan1);

        planStore.updatePlan(planName1, logicalPlan2);

        assertCorrectPlanExists(planName1, logicalPlan2);
    }

    @Test
    public void testDeletePlan() throws TextDBException {
        LogicalPlan logicalPlan1 = LogicalPlanTest.getLogicalPlan1();
        LogicalPlan logicalPlan2 = LogicalPlanTest.getLogicalPlan2();

        String planName1 = "plan1";
        String planName2 = "plan2";

        planStore.addPlan(planName1, "basic regex plan", logicalPlan1);
        planStore.addPlan(planName2, "nlp and regex plan", logicalPlan2);

        planStore.deletePlan(planName1);

        ITuple returnedPlan = planStore.getPlan(planName1);
        Assert.assertNull(returnedPlan);
    }

    @Test
    public void testPlanIterator() throws TextDBException {
        LogicalPlan logicalPlan1 = LogicalPlanTest.getLogicalPlan1();
        LogicalPlan logicalPlan2 = LogicalPlanTest.getLogicalPlan2();
        LogicalPlan logicalPlan3 = LogicalPlanTest.getLogicalPlan3();

        List<LogicalPlan> validPlans = new ArrayList<>();
        validPlans.add(logicalPlan1);
        validPlans.add(logicalPlan2);
        validPlans.add(logicalPlan3);

        List<LogicalPlan> expectedPlans = new ArrayList<>();

        String planNamePrefix = "plan_";

        for (int i = 0; i < 100; i++) {
            LogicalPlan plan = validPlans.get(i % 3);
            expectedPlans.add(plan);
            planStore.addPlan(planNamePrefix + i, "basic plan " + i, plan);
        }


        IDataReader reader = planStore.getPlanIterator();
        reader.open();

        ITuple tuple = null;
        LogicalPlan[] returnedPlans = new LogicalPlan[expectedPlans.size()];

        while ((tuple = reader.getNextTuple()) != null) {
            String planName = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
            int planIdx = Integer.parseInt(planName.split("_")[1]);
            String filePath = tuple.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
            LogicalPlan logicalPlan = planStore.readPlanObject(filePath);
            returnedPlans[planIdx] = logicalPlan;
        }
        reader.close();

        Assert.assertEquals(expectedPlans, Arrays.asList(returnedPlans));
    }

    @Test(expected = TextDBException.class)
    public void testAddPlanWithInvalidName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan/regex";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test(expected = TextDBException.class)
    public void testAddPlanWithEmptyName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test(expected = TextDBException.class)
    public void testAddMultiplePlansWithSameName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test
    public void testDeleteNotExistingPlan() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        planStore.deletePlan(planName + planName);

        assertCorrectPlanExists(planName, logicalPlan);
    }

    @Test
    public void testUpdateNotExistingPlan() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        planStore.updatePlan(planName + planName, "basic regex plan", logicalPlan);

        assertCorrectPlanExists(planName, logicalPlan);
    }

    @Test
    public void testNotDeletePlanBySubstring() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan_sub";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        planStore.deletePlan(planName.substring(0, 4));

        assertCorrectPlanExists(planName, logicalPlan);
    }

}
