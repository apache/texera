package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.storage.relation.RelationManager;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.*;

/**
 * An implementation of query plan store.
 *
 * @author Adrian Seungjin Lee
 */
public class PlanStore {
    private static PlanStore instance = null;
    private RelationManager relationManager = null;

    private PlanStore() throws StorageException, DataFlowException {
        relationManager = RelationManager.getRelationManager();
    }

    public synchronized static PlanStore getInstance() throws StorageException, DataFlowException {
        if (instance == null) {
            instance = new PlanStore();
        }
        return instance;
    }

    /**
     * Creates plan store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void createPlanStore() throws TextDBException {
        if (!relationManager.checkTableExistence(PlanStoreConstants.TABLE_NAME)) {
            relationManager.createTable(PlanStoreConstants.TABLE_NAME,
                    PlanStoreConstants.INDEX_DIR,
                    PlanStoreConstants.SCHEMA_PLAN,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }

        File filesDir = new File(PlanStoreConstants.FILES_DIR);
        if (!filesDir.exists()) {
            filesDir.mkdir();
        }
    }

    /**
     * removes plan store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void destroyPlanStore() throws TextDBException {
        relationManager.deleteTable(PlanStoreConstants.TABLE_NAME);

        Utils.deleteDirectory(PlanStoreConstants.FILES_DIR);
    }

    /**
     * Adds a plan to plan store.
     *
     * @param planName, the name of the plan.
     * @param description, the description of the plan.
     * @param plan, the plan object.
     * @Return IDField, the id field of the plan stored.
     * @throws TextDBException, when there are null fields or the given name is invalid or there is an existing plan with same name.
     */
    public IDField addPlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        if (planName == null || description == null || plan == null) {
            throw new TextDBException("arguments cannot be null when adding a plan");
        }
        if (!PlanStoreConstants.VALID_PLAN_NAME.matcher(planName).find()) {
            throw new TextDBException("plan name is not valid. it can only contain alphanumeric characters, underscore, and hypen.");
        }
        if (getPlan(planName) != null) {
            throw new TextDBException("a plan with the same name already exists");
        }

        String filePath = PlanStoreConstants.FILES_DIR + "/" + planName + PlanStoreConstants.FILE_SUFFIX;
        ITuple tuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                new StringField(planName),
                new StringField(description),
                new StringField(filePath));

        IDField id = relationManager.insertTuple(PlanStoreConstants.TABLE_NAME, tuple);
        writePlanObject(plan, filePath);

        return id;
    }

    /**
     * Retrives a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @Return ITuple, the tuple consisting of fields of the plan.
     * @throws TextDBException
     */
    public ITuple getPlan(String planName) throws TextDBException {
        Query q = new TermQuery(new Term(PlanStoreConstants.NAME, planName));

        IDataReader reader = relationManager.getTuples(PlanStoreConstants.TABLE_NAME, q);
        reader.open();

        ITuple inputTuple = null;

        while ((inputTuple = reader.getNextTuple()) != null) {
            IField nameField = inputTuple.getField(PlanStoreConstants.NAME);
            if (nameField.getValue().toString().equals(planName)) {
                reader.close();
                return inputTuple;
            }
        }

        reader.close();
        return null;
    }

    /**
     * Retrieves an iterator to scan through all plans.
     *
     * @Return IDataReader
     * @throws TextDBException
     */
    public IDataReader getPlanIterator() throws TextDBException {
        return relationManager.scanTable(PlanStoreConstants.TABLE_NAME);
    }

    /**
     * Removess a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @throws TextDBException
     */
    public void deletePlan(String planName) throws TextDBException {
        ITuple plan = getPlan(planName);

        if (plan == null) {
            return;
        }

        IField idField = plan.getField(SchemaConstants._ID);
        relationManager.deleteTuple(PlanStoreConstants.TABLE_NAME, idField);

        IField filePathField = plan.getField(PlanStoreConstants.FILE_PATH);
        deletePlanObject(filePathField.getValue().toString());
    }

    /**
     * Updates plan object of a plan with the given plan name.
     *
     * @param planName, the name of the plan.
     * @param plan, the new plan object.
     * @throws TextDBException
     */
    public void updatePlan(String planName, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, null, plan);
    }

    /**
     * Updates plan description of a plan with the given plan name.
     *
     * @param planName, the name of the plan.
     * @param description, the new description of the plan.
     * @throws TextDBException
     */
    public void updatePlan(String planName, String description) throws TextDBException {
        updatePlanInternal(planName, description, null);
    }

    /**
     * Updates both plan description and plan object of a plan with the given plan name.
     *
     * @param planName, the name of the plan.
     * @param description, the new description of the plan.
     * @param plan, the new plan object.
     * @throws TextDBException
     */
    public void updatePlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, description, plan);
    }

    /**
     * Updates both plan description and plan object of a plan with the given plan name.
     * If description is null, it will not update plan description.
     * If plan is null, it will not update plan object.
     *
     * @param planName, the name of the plan.
     * @param description, the new description of the plan.
     * @param plan, the new plan object.
     * @throws TextDBException
     */
    private void updatePlanInternal(String planName, String description, LogicalPlan plan) throws TextDBException {
        ITuple existingPlan = getPlan(planName);

        if (existingPlan == null) {
            return;
        }

        if (description != null) {
            IField idField = existingPlan.getField(SchemaConstants._ID);
            IField descriptionField = new StringField(description);
            ITuple newTuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                    new StringField(planName),
                    descriptionField,
                    existingPlan.getField(PlanStoreConstants.FILE_PATH));
            relationManager.updateTuple(PlanStoreConstants.TABLE_NAME, newTuple, (IDField) idField);
        }

        if (plan != null) {
            IField filePathField = existingPlan.getField(PlanStoreConstants.FILE_PATH);
            String filePath = filePathField.getValue().toString();
            deletePlanObject(filePath);
            writePlanObject(plan, filePath);
        }
    }

    /**
     * Retrieves plan object stored in given file path.
     *
     * @param filePath, the file path of the plan object.
     * @return LogicalPlan
     * @throws TextDBException
     */
    public LogicalPlan readPlanObject(String filePath) throws TextDBException {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(filePath);
            ois = new ObjectInputStream(fis);
            return (LogicalPlan) ois.readObject();
        } catch (Exception e) {
            throw new TextDBException("failed to read plan object", e);
        } finally {
            try {
                ois.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes the given plan object stored into the given file path.
     *
     * @param plan, the plan object to store.
     * @param filePath, the file path of the plan object.
     * @throws TextDBException
     */
    private void writePlanObject(LogicalPlan plan, String filePath) throws TextDBException {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            fos = new FileOutputStream(filePath);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(plan);
        } catch (IOException e) {
            throw new TextDBException("failed to write plan object", e);
        } finally {
            try {
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Removes a plan object stored in the given file path.
     *
     * @param filePath, the file path of the plan object.
     * @throws TextDBException
     */
    private void deletePlanObject(String filePath) {
        File planFile = new File(filePath);
        if (planFile.exists()) {
            planFile.delete();
        }
    }
}
