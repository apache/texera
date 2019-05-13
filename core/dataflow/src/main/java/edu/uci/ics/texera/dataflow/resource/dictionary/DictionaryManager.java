package edu.uci.ics.texera.dataflow.resource.dictionary;

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
        
        if(! Files.exists(DictionaryManagerConstants.DICTIONARY_DIR_PATH)) {
            try {
                Files.createDirectories(DictionaryManagerConstants.DICTIONARY_DIR_PATH);
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
        StorageUtils.deleteDirectory(DictionaryManagerConstants.DICTIONARY_DIR);
    }
    
    public void addDictionary(String dictID, String dictionaryContent) throws StorageException {
        // write metadata info
        DataWriter dataWriter = relationManager.getTableDataWriter(DictionaryManagerConstants.TABLE_NAME);
        dataWriter.open();
        
        // clean up the same dictionary metadata if it already exists in dictionary table
        dataWriter.deleteTuple(new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));
        
        // insert new tuple
        dataWriter.insertTuple(new Tuple(DictionaryManagerConstants.SCHEMA, new StringField(dictID)));
        
        dataWriter.close();
        
        // write actual dictionary file
        writeToFile(dictID, dictionaryContent);
    }

    public void deleteDictionary(String dictID) {
        DataWriter dataWriter = relationManager.getTableDataWriter(DictionaryManagerConstants.TABLE_NAME);
        dataWriter.open();

        // clean up the same dictionary metadata if it already exists in dictionary table
        dataWriter.deleteTuple(new TermQuery(new Term(DictionaryManagerConstants.NAME, dictID)));

        dataWriter.close();

        try {
            Path filePath = DictionaryManagerConstants.DICTIONARY_DIR_PATH.resolve(dictID);
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new StorageException("Error occurred whlie uploading dictionary");
        }
    }


    /**
     * Write uploaded file at the given location (if the file exists, remove it and write a new one.)
     *
     */
    private void writeToFile(String fileName, String dictionaryContent)  throws StorageException {
        try {
            Path filePath = DictionaryManagerConstants.DICTIONARY_DIR_PATH.resolve(fileName);;
            Files.deleteIfExists(filePath);
            Files.createFile(filePath);
            Files.write(filePath, dictionaryContent.getBytes());
        } catch (IOException e) {
            throw new StorageException("Error occurred whlie uploading dictionary");
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
    
    public String getDictionary(String dictionaryName) throws StorageException {
        DataReader dataReader = relationManager.getTableDataReader(DictionaryManagerConstants.TABLE_NAME, 
                new TermQuery(new Term(DictionaryManagerConstants.NAME, dictionaryName)));
        dataReader.open();
        if (dataReader.getNextTuple() == null) {
            throw new StorageException("Dictionary " + dictionaryName + "does not exist");
        }
        dataReader.close();
        
        try {
            return Files.lines(DictionaryManagerConstants.DICTIONARY_DIR_PATH.resolve(dictionaryName))
                    .collect(Collectors.joining(","));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    
}
