package edu.uci.ics.textdb.exp.operatorstore;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * An implementation of the operator store.
 *
 * @author Kishore Narendran
 */
public class OperatorStore {
    private static OperatorStore instance = null;
    private RelationManager relationManager = null;

    private OperatorStore() throws StorageException, DataFlowException {
        relationManager = RelationManager.getRelationManager();
    }

    public synchronized static OperatorStore getInstance() throws StorageException, DataFlowException {
        if(instance == null) {
            instance = new OperatorStore();
        }
        return instance;
    }

    /**
     * Creates operator store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void createPlanStore() throws TextDBException {
        if (!relationManager.checkTableExistence(OperatorStoreConstants.TABLE_NAME)) {
            relationManager.createTable(OperatorStoreConstants.TABLE_NAME,
                    OperatorStoreConstants.INDEX_DIR,
                    OperatorStoreConstants.SCHEMA_OPERATOR,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }
    }

    /**
     * removes operator store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void destroyPlanStore() throws TextDBException {
        relationManager.deleteTable(OperatorStoreConstants.TABLE_NAME);
    }
}
