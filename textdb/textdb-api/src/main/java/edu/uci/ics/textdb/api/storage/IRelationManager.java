package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;

/**
 * IRelationManager is the interface for TextDB's relation manager, 
 *   which manages the meta data information about each table, 
 *   and provides functions to manipulate tables.
 * 
 * @author Zuozhi Wang
 */
public interface IRelationManager {
        
    public boolean checkTableExistence(String tableName);
    
    // create a new table, tableName must be unique
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzer) throws TextDBException;
    
    // drop a table
    public void deleteTable(String tableName) throws TextDBException;
    
    // insert a tuple to a table, returns the ID field
    public IField insertTuple(String tableName, ITuple tuple) throws TextDBException;
    
    // delete a tuple by its id
    public void deleteTuple(String tableName, IField idValue) throws TextDBException;
    
    // update a tuple by its id
    public void updateTuple(String tableName, ITuple newTuple, IField idValue) throws TextDBException;
    
    // get a tuple by its id
    public ITuple getTuple(String tableName, IField idValue) throws TextDBException;
    
    // get the dataReader to scan a table
    public IDataReader scanTable(String tableName) throws TextDBException;
    
    public String getTableDirectory(String tableName) throws TextDBException;
    
    public Schema getTableSchema(String tableName) throws TextDBException;
    
    public String getTableAnalyzer(String tableName) throws TextDBException;
    
}
