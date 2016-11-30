package edu.uci.ics.textdb.storage.relation;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;

public class RelationTest {
    
    RelationManager relationManager;
    
    @Before
    public void setUpRelationManager() throws TextDBException {
        relationManager = RelationManager.getRelationManager();
    }
    
    /*
     * Test the information about "collection catalog" itself is stored properly.
     * 
     */
    @Test
    public void test1() throws Exception {
        String collectionCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.COLLECTION_CATALOG);
        String collectionCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.COLLECTION_CATALOG);
        Schema collectionCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.COLLECTION_CATALOG);
                
        Assert.assertEquals(collectionCatalogDirectory, 
                new File(CatalogConstants.COLLECTION_CATALOG_DIRECTORY).getAbsolutePath());
        Assert.assertEquals(collectionCatalogLuceneAnalyzer, LuceneAnalyzerConstants.standardAnalyzerString());
        Assert.assertEquals(collectionCatalogSchema, CatalogConstants.COLLECTION_CATALOG_SCHEMA);
    }
    
    /*
     * Test the information about "schema catalog" itself is stored properly.
     */
    @Test
    public void test2() throws Exception {
        String schemaCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.SCHEMA_CATALOG);
        String schemaCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.SCHEMA_CATALOG);
        Schema schemaCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.SCHEMA_CATALOG);
        
        Assert.assertEquals(schemaCatalogDirectory, 
                new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY).getAbsolutePath());
        Assert.assertEquals(schemaCatalogLuceneAnalyzer, LuceneAnalyzerConstants.standardAnalyzerString());
        Assert.assertEquals(schemaCatalogSchema, CatalogConstants.SCHEMA_CATALOG_SCHEMA);  
    }
    
}
