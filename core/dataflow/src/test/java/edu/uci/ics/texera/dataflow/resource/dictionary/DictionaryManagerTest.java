package edu.uci.ics.texera.dataflow.resource.dictionary;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;

public class DictionaryManagerTest {
    
    /*
     * Test connection & createDictionaryManager
     */
    @Test
    public void connectionTest() throws TexeraException {
        DictionaryManager.getInstance();
    }

    /*
     * Test adding dictionary
     */
    @Test
    public void addDictionaryTest() throws TexeraException {
        DictionaryManager.getInstance().addDictionary("dict1", "The content of dict1.");
        DictionaryManager.getInstance().addDictionary("dict2", "The content of dict2.");
    }

   /*
    * Test getDictionaries
    */
   @Test
   public void getDictionariesTest() throws TexeraException {
       Assert.assertEquals("[dict1, dict2]", DictionaryManager.getInstance().getDictionaries().toString());
   }

   /*
    * Test getDictionary
    */
   @Test
   public void getDictionaryTest() throws TexeraException {
       Assert.assertEquals("The content of dict1.", DictionaryManager.getInstance().getDictionary("dict1"));
       Assert.assertEquals("The content of dict2.", DictionaryManager.getInstance().getDictionary("dict2"));
   }

   /*
    * Test destroyDictionaryManager
    */
   @Test
   public void destroyDictionaryManagerTest() throws TexeraException {
       DictionaryManager.getInstance().destroyDictionaryManager();
       Assert.assertFalse(DictionaryManager.getInstance().checkTableExistence(DictionaryManagerConstants.TABLE_NAME));
   }

}