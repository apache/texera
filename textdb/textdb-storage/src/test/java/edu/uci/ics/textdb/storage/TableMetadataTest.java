package edu.uci.ics.textdb.storage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by SmartGUI team on 5/5/17.
 */
public class TableMetadataTest {
    private TableMetadata tableMetadata;
    private RelationManager relationManager;
    private List<TableMetadata> result;

    @Before
    public void setUp() throws Exception {
        System.out.println("\n");
        relationManager = RelationManager.getRelationManager();
        result = relationManager.getMetaData();
        tableMetadata = new TableMetadata(result.get(0).getTableName(), result.get(0).getSchema());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getTableName() throws Exception {
        Assert.assertEquals("promed", tableMetadata.getTableName());
    }

    @Test
    public void getSchema() throws Exception {
        Assert.assertEquals(result.get(0).getSchema(), tableMetadata.getSchema());
    }

}