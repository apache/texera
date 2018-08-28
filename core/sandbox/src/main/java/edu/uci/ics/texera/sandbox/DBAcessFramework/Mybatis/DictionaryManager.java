package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import org.apache.ibatis.session.*;
import org.apache.ibatis.io.Resources;
import java.io.InputStream;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private static SqlSessionFactory factory = null;

    private DictionaryManager() throws StorageException {
        // Establish mybatis connection to MySQL
        try {
            String resource = "mybatis-config.xml";
            InputStream inputStream = Resources.getResourceAsStream(resource);
            factory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (Exception e) {e.printStackTrace();}
    }


    public synchronized static DictionaryManager getInstance() throws StorageException {
        if (instance == null) {
            instance = new DictionaryManager();
            instance.createDictionaryManager();
        }
        return instance;
    }

    /**
     * Creates plan store, both an index and a directory for plan objects.
     *
     * @throws TexeraException
     */
    public void createDictionaryManager() throws TexeraException {
        SqlSession session = factory.openSession();
        try {
            session.update("DictionaryMapper.xml.createTable");
        }
        finally {
            session.close();
        }
    }

   /**
    * removes plan store, both an index and a directory for dictionary objects.
    *
    * @throws TexeraException
    */
   public void destroyDictionaryManager() throws TexeraException {
       SqlSession session = factory.openSession();
       try {
           session.update("DictionaryMapper.xml.dropTable");
       }
       finally {
           session.close();
       }
   }

    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        SqlSession session = factory.openSession();
        try {
            HashMap<String, String> hs = new HashMap<String, String>();
            hs.put("name", fileName);
            hs.put("content", dictionaryContent);
            session.insert("DictionaryMapper.xml.insertDict", hs);
        } finally {
            session.close();
        }
    }

   public List<String> getDictionaries() throws StorageException {
       List<String> dictionaries = new ArrayList<String>();
       List<Dictionary> dictList = null;
       SqlSession session = factory.openSession();
       try {
           dictList = session.selectList("DictionaryMapper.xml.selectDicts");
       } finally {
           session.close();
       }
       for (Dictionary d : dictList)
           dictionaries.add(d.getName());
       return dictionaries;
   }

   public String getDictionary(String dictionaryName) throws StorageException {
       Dictionary dict = null;
       SqlSession session = factory.openSession();
       try {
           dict = session.selectOne("DictionaryMapper.xml.selectDict", dictionaryName);
       } finally {
           session.close();
       }
       return dict.getContent();
   }

}