package edu.uci.ics.textdb.storage.catalog;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import junit.framework.Assert;

/**
 * Test cases for CatalogManager
 * 
 * @author Zuozhi Wang
 *
 */
public class CatalogManagerTest {
    
    @Before
    public void setUp() throws Exception {
        if (! CatalogManager.isCatalogManagerExist()) {
            CatalogManager.createCatalog();
        }
    }
    
    /*
     * Test the information about "collection catalog" itself is stored properly.
     * 
     */
    @Test
    public void test1() throws Exception {
        String collectionCatalogDirectory = 
                CatalogManager.getCollectionDirectory(CatalogConstants.COLLECTION_CATALOG);
        String collectionCatalogLuceneAnalyzer = 
                CatalogManager.getCollectionLuceneAnalyzer(CatalogConstants.COLLECTION_CATALOG);
        Schema collectionCatalogSchema = 
                CatalogManager.getCollectionSchema(CatalogConstants.COLLECTION_CATALOG);
                
        Assert.assertEquals(collectionCatalogDirectory, 
                new File(CatalogConstants.COLLECTION_CATALOG_DIRECTORY).getAbsolutePath());
        Assert.assertEquals(collectionCatalogLuceneAnalyzer, DataConstants.STANDARD_LUCENE_ANALYZER);
        Assert.assertEquals(collectionCatalogSchema, CatalogConstants.COLLECTION_CATALOG_SCHEMA);
    }
    
    /*
     * Test the information about "schema catalog" itself is stored properly.
     */
    @Test
    public void test2() throws Exception {
        String schemaCatalogDirectory = 
                CatalogManager.getCollectionDirectory(CatalogConstants.SCHEMA_CATALOG);
        String schemaCatalogLuceneAnalyzer = 
                CatalogManager.getCollectionLuceneAnalyzer(CatalogConstants.SCHEMA_CATALOG);
        Schema schemaCatalogSchema = 
                CatalogManager.getCollectionSchema(CatalogConstants.SCHEMA_CATALOG);
        
        Assert.assertEquals(schemaCatalogDirectory, 
                new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY).getAbsolutePath());
        Assert.assertEquals(schemaCatalogLuceneAnalyzer, DataConstants.STANDARD_LUCENE_ANALYZER);
        Assert.assertEquals(schemaCatalogSchema, CatalogConstants.SCHEMA_CATALOG_SCHEMA);  
    }
    
    /*
     * Add a collection's information to the catalog 
     * and test if information can be retrieved successfully.
     */
    @Test
    public void test3() throws Exception {
        String collection1Name = "catalog_manager_test_collection_1";
        String collection1Directory = "./index/test_collection_1/";
        Schema collection1Schema = new Schema(
                new Attribute("id", FieldType.INTEGER), new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        String collection1LuceneAnalyzer = DataConstants.STANDARD_LUCENE_ANALYZER;
        
        CatalogManager.putCollectionSchema(collection1Name, collection1Directory, collection1Schema, collection1LuceneAnalyzer);
        
        Assert.assertEquals(new File(collection1Directory).getCanonicalPath(), 
                CatalogManager.getCollectionDirectory(collection1Name));
        Assert.assertEquals(collection1Schema, CatalogManager.getCollectionSchema(collection1Name));
        Assert.assertEquals(collection1LuceneAnalyzer, CatalogManager.getCollectionLuceneAnalyzer(collection1Name));
        
        CatalogManager.deleteCollectionCatalog(collection1Name);
    }
    
    /*
     * Retrieving the directory of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test4() throws Exception {
        String collectionName = "catalog_manager_test_collection_1";
        CatalogManager.getCollectionDirectory(collectionName);
    }
    
    /*
     * Retrieving the lucene analyzer of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test5() throws Exception {
        String collectionName = "catalog_manager_test_collection_1";
        CatalogManager.getCollectionLuceneAnalyzer(collectionName);
    }
    
    /*
     * Retrieving the schema of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test6() throws Exception {
        String collectionName = "catalog_manager_test_collection_1";
        CatalogManager.getCollectionSchema(collectionName);
    }
    
    
    /*
     * Test inserting and deleting multiple collections to the catalog.
     */
    @Test
    public void test7() throws Exception {
        String collectionName = "catalog_manager_test_collection";
        String collectionDirectory = "./index/test_collection";
        Schema collectionSchema = new Schema(
                new Attribute("id", FieldType.INTEGER), new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        
        int NUM_OF_LOOPS = 20;
        
        // insert collections
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            CatalogManager.putCollectionSchema(collectionName + '_' + i, collectionDirectory + '_' + i, 
                    collectionSchema, DataConstants.STANDARD_LUCENE_ANALYZER);
        }
        // assert collections are correctly inserted
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            Assert.assertEquals(new File(collectionDirectory + '_' + i).getCanonicalPath(), 
                    CatalogManager.getCollectionDirectory(collectionName + '_' + i));
            Assert.assertEquals(collectionSchema, CatalogManager.getCollectionSchema(collectionName + '_' + i));
            Assert.assertEquals(DataConstants.STANDARD_LUCENE_ANALYZER, 
                    CatalogManager.getCollectionLuceneAnalyzer(collectionName + '_' + i));
        }
        // delete collections
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            CatalogManager.deleteCollectionCatalog(collectionName + '_' + i);
        }
        // assert collections are correctly deleted
        int errorCount = 0;
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            try {
                CatalogManager.getCollectionDirectory(collectionName + '_' + i);
            } catch (StorageException e) {
                Assert.assertEquals(i, errorCount);
                errorCount++;
            }
        }
    }
    
}
