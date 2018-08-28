package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.storage.utils.StorageUtils;
import edu.uci.ics.texera.storage.constants.MySQLConstants;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private static SessionFactory factory = null;

    private DictionaryManager() throws StorageException {
        // Establish hibernate connection to MySQL
        Configuration cfg = new Configuration().configure();

        factory = cfg.buildSessionFactory();

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
        Session session = null;

        try {
            session = factory.openSession();

            session.beginTransaction();

            session.getTransaction().commit();

        } catch (Exception e) {
            e.printStackTrace();
            session.getTransaction().rollback();
        } finally {
            if(session != null)
                if (session.isOpen())
                    session.close();
        }
    }

   /**
    * removes plan store, both an index and a directory for dictionary objects.
    *
    * @throws TexeraException
    */
   public void destroyDictionaryManager() throws TexeraException {
        Session session = null;

        try {
            session = factory.openSession();
            session.beginTransaction();
            int result = session.createQuery("delete edu.uci.ics.texera.dataflow.resource.dictionary.Dictionary").executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
            session.getTransaction().rollback();
        } finally {
            if(session != null)
                if (session.isOpen())
                    session.close();
        }
        
   }

    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        Session session = null;
        try{
            session = factory.openSession();

            session.beginTransaction();

            Dictionary dict = new Dictionary();
            dict.setId(fileName);
            dict.setContent(dictionaryContent);

            session.save(dict);

            session.getTransaction().commit();

        }catch(Exception e){
            e.printStackTrace();

            session.getTransaction().rollback();
        }finally{
            if(session != null)
                if(session.isOpen())
                    session.close();
        }
    }

   public List<String> getDictionaries() throws StorageException {
       List<String> dictionaries = new ArrayList<String>();
       Session session = null;
       try{
            session = factory.openSession();
            List<Object> result = session.createQuery("from Dictionary").list();
            for (Iterator<Object> iterator = result.iterator();iterator.hasNext();) {
                Dictionary dict = (Dictionary) iterator.next();
                dictionaries.add(dict.getId());
            }

       }catch(Exception e){
           e.printStackTrace();

           session.getTransaction().rollback();
       }finally{
           if(session != null)
               if(session.isOpen())
                   session.close();
       }
       return dictionaries;
   }

   public String getDictionary(String dictionaryName) throws StorageException {
        String dictionaryContent = null;
        Session session = null;
        try{
            session = factory.openSession();
            Query query = session.createQuery("from Dictionary where id = :name");
            query.setParameter("name", dictionaryName);
            List<Object> result = query.list();
            Iterator<Object> iterator = result.iterator();
            if (iterator.hasNext()) {
                Dictionary dict = (Dictionary) iterator.next();
                dictionaryContent = dict.getContent();
            }
        }catch(Exception e){
            e.printStackTrace();

            session.getTransaction().rollback();
        }finally{
            if(session != null)
                if(session.isOpen())
                    session.close();
        }
        return dictionaryContent;
   }

}