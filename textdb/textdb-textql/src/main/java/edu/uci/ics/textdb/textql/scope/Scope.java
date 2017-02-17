package edu.uci.ics.textdb.textql.scope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.relation.RelationManager;
import edu.uci.ics.textdb.textql.statements.Statement;

/**
 * Scope to be used by the TextQL Language Parser. The scope is capable of
 * store declared schemas and statements (including its generated schemas).
 * A RelationManager can be used as an alternative persistent source of Schemas.
 * 
 * @author Flavio Bayer
 *
 */
public class Scope {

    /**
     * The Relation Manager used to fetch table schemas.
     */
    private final RelationManager relationManager;

    /**
     * Collection for all declared schemas in the Scope. The key of the map is
     * the name of the schema and the value is the schema itself. 
     */
    private final LinkedHashMap<String, Schema> schemasByName;

    /**
     * Collection for declared statements. The key of the map is the id of the
     * statement (that is, the value returned by the getId method of the
     * Statement) and the value is the statement itself. 
     */
    private final LinkedHashMap<String, Statement> statementById;

    /**
     * Initialize the scope without the use of a RelationManager.
     */
    public Scope() {
        this(null);
    }

    /**
     * Initialize the Scope with the given RelationManager.
     * 
     * @param relationManager
     *            The RelationManager to be used in the scope.
     */
    public Scope(RelationManager relationManager) {
        this.relationManager = relationManager;
        this.statementById = new LinkedHashMap<>();
        this.schemasByName = new LinkedHashMap<>();
    }

    /**
     * Return the RelationManager being used by the Scope.
     * 
     * @return The RelationManager currently being used.
     */
    public RelationManager getRealationManager() {
        return relationManager;
    }

    /**
     * Add a Schema to the scope.
     * 
     * @param schemaName The name of the schema to be added (must be unique).
     * @param schema The schema to be added.
     * @throws TextDBException If the schema cannot be added.
     */
    public void addSchema(String schemaName, Schema schema) throws TextDBException {
        // Assert the name of the schema is valid and it is not yet associated
        scopeAssert(schemaName != null, "The name of the schema is mandatory");
        scopeAssert(!schemaName.isEmpty(), "The name of the schema is mandatory");
        scopeAssert(getSchema(schemaName) == null, "Schema for '" + schemaName + "' is already defined");

        // Add the schema into the Scope
        schemasByName.put(schemaName, schema);
    }

    /**
     * Get the schema associated with the given schemaName. The schema is
     * consecutively looked into the Collection of declared schemas and the
     * RelationManager until it is found. If the schema can be found in the
     * RelationManager, it is added to the Collection of declared Schemas.
     * If the scope cannot find the schema with the given name, then null is
     * returned.
     * 
     * @param schemaName The name of the schema to look for.
     * @return The schema associated with the given name.
     * @throws StorageException
     *             If an error occur in the RelationManager.
     */
    public Schema getSchema(String schemaName) throws StorageException {
        Schema schema = null;
        if (schemasByName.containsKey(schemaName)) {
            // Look for the schema in the Map of declared schemas
            schema = schemasByName.get(schemaName);
        } else if (relationManager != null && relationManager.checkTableExistence(schemaName)) {
            // Look for the schema as a table in the RelationManager and add it to the scope
            schema = relationManager.getTableSchema(schemaName);
            schemasByName.put(schemaName, schema);
        }
        // Return the schema found, null otherwise
        return schema;
    }

    /**
     * Get a map containing the schemas inserted in the scope. The Entry of the
     * map is in the format <SchemaName,Schema>. Schemas inserted in the scope
     * by the addition of statements are also returned. The returned Map can be
     * iterated in the same order in which the Schemas were added.
     * 
     * @return An unmodifiable Map containing all the inserted schemas by name.
     */
    public Map<String, Schema> getSchemas() {
        return Collections.unmodifiableMap(schemasByName);
    }

    /**
     * Add a new Statement and its generated output Schema to the scope.
     * 
     * @param statement The statement to be added to the scope (the Id of the
     *            statement must be unique).
     * @throws StorageException If an error occur while trying to fetch a 
     *            table schema.
     * @throws TextDBException If an error occur when a statement is generating
     *            the output schema.
     */
    public void addStatement(Statement statement) throws StorageException, TextDBException {
        // Assert the id of the statement is valid and it is not yet associated
        scopeAssert(statement.getId() != null, "The ID attribute is mandatory for a statement");
        scopeAssert(!statement.getId().isEmpty(), "The ID attribute is mandatory for a statement");
        scopeAssert(!statementById.containsKey(statement.getId()),
                "View with name '" + statement.getId() + "' is already declared");
        scopeAssert(getSchema(statement.getId()) == null, "Schema for '" + statement.getId() + "' is already defined");

        // Compute the output schema of the statement
        List<String> inputViews = statement.getInputViews();
        List<Schema> inputSchemas = new ArrayList<>(inputViews.size());
        for (String inputViewName : inputViews) {
            Schema inputSchema = getSchema(inputViewName);
            scopeAssert(inputSchema != null, "Unable to find required schema '" + inputViewName + "' for statement");
            inputSchemas.add(inputSchema);
        }
        Schema outputSchema = statement.generateOutputSchema(inputSchemas.stream().toArray(Schema[]::new));

        // Add the statement and the generated Schema into the Scope
        statementById.put(statement.getId(), statement);
        schemasByName.put(statement.getId(), outputSchema);
    }

    /**
     * Get a statement with Id equals to the given statementId. Null is returned
     * if no statement is found with the given Id.
     * 
     * @param statementId The Id of the statement to search for.
     * @return The statement associated with the given id, or null if no statement is
     *         found.
     */
    public Statement getStatement(String statementId) {
        return statementById.get(statementId);
    }

    /**
     * Return a collection containing all the valid statements added to this
     * scope. The returned collection can be iterated in the same order in which
     * the statements were added.
     * 
     * @return A collection with valid declared statements.
     */
    public Collection<Statement> getStatements() {
        return statementById.values();
    }

    /**
     * Assert the value of assertBoolean is true or throw a TextDBException.
     * 
     * @param assertBoolean If a TextDBException should not be thrown.
     * @param errorMessage The error message to describe a TextDBException
     *            if it is thrown.
     * @throws TextDBException If the value of assertBoolean is false.
     */
    private static void scopeAssert(boolean assertBoolean, String errorMessage) throws TextDBException {
        if (!assertBoolean) {
            throw new TextDBException(errorMessage);
        }
    }

}
