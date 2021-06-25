package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import edu.uci.ics.texera.storage.utils.StorageUtils;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private RelationManager relationManager = null;

    private DictionaryManager() throws StorageException {
        relationManager = RelationManager.getInstance();
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
        if (! relationManager.checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
            relationManager.createTable(DictionaryManagerConstants.TABLE_NAME,
                    DictionaryManagerConstants.INDEX_DIR,
                    DictionaryManagerConstants.SCHEMA,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }
        
        if(! Files.exists(DictionaryManagerConstants.DICTIONARY_CONTENT_DIR_PATH)) {
            try {
                Files.createDirectories(DictionaryManagerConstants.DICTIONARY_CONTENT_DIR_PATH);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        
        if(! Files.exists(DictionaryManagerConstants.DICTIONARY_NAME_DIR_PATH)) {
            try {
                Files.createDirectories(DictionaryManagerConstants.DICTIONARY_NAME_DIR_PATH);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        
        if(! Files.exists(DictionaryManagerConstants.DICTIONARY_DESCRIPTION_DIR_PATH)) {
            try {
                Files.createDirectories(DictionaryManagerConstants.DICTIONARY_DESCRIPTION_DIR_PATH);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
    }

    /**
     * removes plan store, both an index and a directory for dictionary objects.
     *
     * @throws TexeraException
     */
    public void destroyDictionaryManager() throws TexeraException {
        relationManager.deleteTable(DictionaryManagerConstants.TABLE_NAME);
        StorageUtils.deleteDirectory(DictionaryManagerConstants.DICTIONARY_CONTENT_DIR);
    }
    
    public void addDictionary(String dictID, String dictionaryContent, String dictionaryName, String dictionaryDescription) throws StorageException {
        // write metadata info
        DataWriter dataWriter = relationManager.getTableDataWriter(DictionaryManagerConstants.TABLE_NAME);
        dataWriter.open();
        
        // clean up the same dictionary metadata if it already exists in dictionary table
        dataWriter.deleteTuple(new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));
        
        // insert new tuple
        dataWriter.insertTuple(new Tuple(DictionaryManagerConstants.SCHEMA, new StringField(dictID)));
        
        dataWriter.close();
        
        // write actual dictionary file
        writeContentToFile(dictID, dictionaryContent);
        writeNameToFile(dictID, dictionaryName);
        writeDescriptionToFile(dictID, dictionaryDescription);
    }

    public void deleteDictionary(String dictID) {
        DataWriter dataWriter = relationManager.getTableDataWriter(DictionaryManagerConstants.TABLE_NAME);
        dataWriter.open();

        // clean up the same dictionary metadata if it already exists in dictionary table
        dataWriter.deleteTuple(new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));

        dataWriter.close();

    }


    /**
     * Write uploaded file at the given location (if the file exists, remove it and write a new one.)
     *
     */
    private void writeContentToFile(String dictID, String dictionaryContent)  throws StorageException {
        try {
            Path filePath = DictionaryManagerConstants.DICTIONARY_CONTENT_DIR_PATH.resolve(dictID);
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toString()));
            writer.write(dictionaryContent);
            writer.close();
        } catch (IOException e) {
            throw new StorageException("Error occurred whlie uploading dictionary content");
        }
    }
    
    /**
     * Write uploaded file at the given location (if the file exists, remove it and write a new one.)
     *
     */
    private void writeNameToFile(String dictID, String dictionaryName)  throws StorageException {
        try {
            Path filePath = DictionaryManagerConstants.DICTIONARY_NAME_DIR_PATH.resolve(dictID);;
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toString()));
            writer.write(dictionaryName);
            writer.close();
        } catch (IOException e) {
            throw new StorageException("Error occurred whlie uploading dictionary name");
        }
    }
    
    /**
     * Write uploaded file at the given location (if the file exists, remove it and write a new one.)
     *
     */
    private void writeDescriptionToFile(String dictID, String dictionaryDescription)  throws StorageException {
        try {
            Path filePath = DictionaryManagerConstants.DICTIONARY_DESCRIPTION_DIR_PATH.resolve(dictID);;
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toString()));
            writer.write(dictionaryDescription);
            writer.close();
        } catch (IOException e) {
            throw new StorageException("Error occurred whlie uploading dictionary description");
        }
    }
    
    public List<String> getDictionaryIDs() throws StorageException {
        List<String> dictionaries = new ArrayList<>();
        
        DataReader dataReader = relationManager.getTableDataReader(DictionaryManagerConstants.TABLE_NAME, new MatchAllDocsQuery());
        dataReader.open();
        
        Tuple tuple;
        while ((tuple = dataReader.getNextTuple()) != null) {
            dictionaries.add(tuple.getField(DictionaryManagerConstants.NAME).getValue().toString());
        }
        dataReader.close();
        
        return dictionaries;
    }
    
    public String getDictionaryContent(String dictID) throws StorageException {
        DataReader dataReader = relationManager.getTableDataReader(DictionaryManagerConstants.TABLE_NAME, 
                new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));
        dataReader.open();
        if (dataReader.getNextTuple() == null) {
            throw new StorageException("Dictionary " + dictID + "does not exist");
        }
        dataReader.close();
        
        try {
            return Files.lines(DictionaryManagerConstants.DICTIONARY_CONTENT_DIR_PATH.resolve(dictID))
                    .collect(Collectors.joining(","));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    
    public String getDictionaryName(String dictID) throws StorageException {
        DataReader dataReader = relationManager.getTableDataReader(DictionaryManagerConstants.TABLE_NAME, 
                new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));
        dataReader.open();
        if (dataReader.getNextTuple() == null) {
            throw new StorageException("Dictionary " + dictID + "does not exist");
        }
        dataReader.close();
        
        try {
            return Files.lines(DictionaryManagerConstants.DICTIONARY_NAME_DIR_PATH.resolve(dictID))
                    .collect(Collectors.joining(""));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    
    public String getDictionaryDescription(String dictID) throws StorageException {
        DataReader dataReader = relationManager.getTableDataReader(DictionaryManagerConstants.TABLE_NAME, 
                new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));
        dataReader.open();
        if (dataReader.getNextTuple() == null) {
            throw new StorageException("Dictionary " + dictID + "does not exist");
        }
        dataReader.close();
        
        try {
            return Files.lines(DictionaryManagerConstants.DICTIONARY_DESCRIPTION_DIR_PATH.resolve(dictID))
                    .collect(Collectors.joining(""));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    
    public static void main(String args) {
    	System.out.println("HEllo WOrld");
    }
    
}
