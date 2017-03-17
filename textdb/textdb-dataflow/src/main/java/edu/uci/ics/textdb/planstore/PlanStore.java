package edu.uci.ics.textdb.planstore;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * An implementation of query plan store.
 *
 * @author Adrian Seungjin Lee
 * @author Kishore Narendran
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
    }

    /**
     * removes plan store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void destroyPlanStore() throws TextDBException {
        relationManager.deleteTable(PlanStoreConstants.TABLE_NAME);
    }

    /**
     * Adds a Logical Plan JSON to the plan store.
     *
     * @param planName, the name of the plan.
     * @param description, the description of the plan.
     * @param logicalPlanJson, the logical plan JSON string
     * @Return IDField, the id field of the plan stored.
     * @throws TextDBException, when there are null fields or the given name is invalid or there is an existing plan with same name.
     */
    public IDField addPlan(String planName, String description, String logicalPlanJson) throws TextDBException {
        if (planName == null || description == null || logicalPlanJson == null) {
            throw new TextDBException("Arguments cannot be null when adding a plan");
        }
        if (!PlanStoreConstants.VALID_PLAN_NAME.matcher(planName).find()) {
            throw new TextDBException("Plan name is not valid. It can only contain alphanumeric characters, " +
                    "underscore, and hyphen.");
        }
        if (getPlan(planName) != null) {
            throw new TextDBException("A plan with the same name already exists");
        }

        // Converting the JSON String to a JSON Node to minimize space usage and to check validity of JSON string
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(logicalPlanJson).getAsJsonObject();

        Tuple tuple = new Tuple(PlanStoreConstants.SCHEMA_PLAN,
                new StringField(planName),
                new StringField(description),
                new StringField(jsonObject.toString()));

        DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);
        dataWriter.open();
        IDField id = dataWriter.insertTuple(tuple);
        dataWriter.close();

        return id;
    }

    /**
     * Retrieves a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @Return ITuple, the tuple consisting of fields of the plan.
     * @throws TextDBException
     */
    public Tuple getPlan(String planName) throws TextDBException {
        Query q = new TermQuery(new Term(PlanStoreConstants.NAME, planName));

        DataReader reader = relationManager.getTableDataReader(PlanStoreConstants.TABLE_NAME, q);
        reader.open();

        Tuple inputTuple = null;

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
    public DataReader getPlanIterator() throws TextDBException {
        return relationManager.getTableDataReader(PlanStoreConstants.TABLE_NAME, new MatchAllDocsQuery());
    }

    /**
     * Removess a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @throws TextDBException
     */
    public void deletePlan(String planName) throws TextDBException {
        Tuple plan = getPlan(planName);

        if (plan == null) {
            return;
        }

        IDField idField = (IDField) plan.getField(SchemaConstants._ID);
        
        DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);        
        dataWriter.open();
        dataWriter.deleteTupleByID(idField);
        dataWriter.close();
    }

    /**
     * Updates the description for the given plan name
     * @param planName - Name of the plan whose description is to be modified
     * @param description - New description of the plan as it it is to be updated
     * @throws TextDBException
     */
    public void updatePlanDescription(String planName, String description) throws TextDBException{
        updatePlanInternal(planName, description, null);
    }

    /**
     * Updates the logical plan for the given plan name
     * @param planName - Name of the plan which is to be modified
     * @param logicalPlanJson - New logical plan json as it is to be updated in the plan store
     * @throws TextDBException
     */
    public void updatePlan(String planName, String logicalPlanJson) throws TextDBException{
        updatePlanInternal(planName, null, logicalPlanJson);

    }

    /**
     * Updates both the description and the logical plan json for the given plan name
     * @param planName - Name of the plan which is to be updated
     * @param description - New description for the plan
     * @param logicalPlanJson - New logical plan json for the plan
     * @throws TextDBException
     */
    public void updatePlan(String planName, String description, String logicalPlanJson) throws TextDBException{
        updatePlanInternal(planName, description, logicalPlanJson);
    }

    /**
     * Updates both plan description and plan json of a plan with the given plan name.
     * If description is null, it will not update plan description.
     * If plan json is NULL, it will not update the plan's JSON file.
     *
     * @param planName, the name of the plan.
     * @param description, the new description of the plan.
     * @param logicalPlanJson, the new plan json string.
     * @throws TextDBException
     */
    private void updatePlanInternal(String planName, String description, String logicalPlanJson) throws TextDBException{
        Tuple existingPlan = getPlan(planName);

        if (existingPlan == null) {
            return;
        }

        // Checking if an updated description or logical plan JSON string has been provided
        if(description == null && logicalPlanJson == null) {
            return;
        }

        // Checking if the logical plan JSON string needs to be updated
        if(logicalPlanJson != null) {
            // Compressing and checking the validity of the logical plan JSON string
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(logicalPlanJson).getAsJsonObject();
            logicalPlanJson = jsonObject.toString();
        }

        // Getting the fields in order for performing the update
        IDField idField = (IDField) existingPlan.getField(SchemaConstants._ID);
        IField descriptionField = description != null ?
                new StringField(description) : existingPlan.getField(PlanStoreConstants.DESCRIPTION);
        IField logicalPlanJsonField = logicalPlanJson != null ?
                new StringField(logicalPlanJson) : existingPlan.getField(PlanStoreConstants.LOGICAL_PLAN_JSON);

        // Creating a tuple out of all the fields
        Tuple newTuple = new Tuple(PlanStoreConstants.SCHEMA_PLAN,
                new StringField(planName),
                descriptionField,
                logicalPlanJsonField);

        // Writing the updated tuple
        DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);
        dataWriter.open();
        dataWriter.updateTuple(newTuple, idField);
        dataWriter.close();
    }
}
