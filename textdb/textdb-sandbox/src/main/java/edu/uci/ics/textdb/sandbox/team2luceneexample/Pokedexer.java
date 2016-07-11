package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by shiladityasen on 06/04/16.
 */

import static java.nio.file.Files.delete;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Pokedexer {

    //Data members of the Pokemon Indexer class
    private IndexWriter indexWriter;
    private int numberOfPokemon;
    private String indexDirectoryPath;

    /**
     * Parametrized constructor for the indexer class
     * @param indexDirectoryPath - Takes the path to where the index is to be built/already exists
     * @param numberOfPokemon - Takes the number of pokemon whose information the index is to haev
     * @throws IOException
     */
    public Pokedexer(String indexDirectoryPath, int numberOfPokemon) throws IOException {
        Directory direc = FSDirectory.open(Paths.get(indexDirectoryPath));
        IndexWriterConfig ixWriteConf = new IndexWriterConfig(new StandardAnalyzer());
        indexWriter = new IndexWriter(direc, ixWriteConf);
        this.numberOfPokemon = numberOfPokemon;
        this.indexDirectoryPath = indexDirectoryPath;
    }

    /**
     * Getter function that returns the value of a the numberOfPokemon variable
     * @return - int - value of the numberOfPokemon variable
     */
    public int getNumberOfPokemon() {
        return numberOfPokemon;
    }

    /**
     * Function that makes and returns a Document on accepting a Pokemon object.
     * @param pokemon - pokemon object that needs to be converted to a document
     * @return - Document - Document version of the pokemon object
     * @throws IOException
     */
    private Document makeDocument(Pokemon pokemon) throws IOException {
        Document doc = new Document();

        //index pokemon info
        Field id = new IntField(LuceneConstants.ID_FIELD, pokemon.getId(), Field.Store.YES);
        Field name = new StringField(LuceneConstants.NAME_FIELD, pokemon.getName(), Field.Store.YES);

//        ArrayList<String> all_types = new ArrayList<String>(Arrays.asList(pokemon.getTypes().split("\\s*,\\s*")));
//        ArrayList<Field> types = new ArrayList();
//        for (int i = 0; i < types.size(); i++)
//            types.add(new StringField(LuceneConstants.TYPES_FIELD, all_types.get(i), Field.Store.YES));
        Field types = new TextField(LuceneConstants.TYPES_FIELD, pokemon.getTypes(), Field.Store.YES);

//        ArrayList<String> all_moves = new ArrayList(Arrays.asList(pokemon.getMoves().split("\\s*,\\s*")));
//        ArrayList<Field> moves = new ArrayList();
//        for (int i = 0; i < moves.size(); i++)
//            moves.add(new StringField(LuceneConstants.MOVES_FIELD, all_moves.get(i), Field.Store.NO));
        Field moves = new TextField(LuceneConstants.MOVES_FIELD, pokemon.getMoves(), Field.Store.NO);

        //add fields to doc
        doc.add(id);
        doc.add(name);
//        for (int i = 0; i < types.size(); i++)
//            doc.add(types.get(i));
//        for (int i = 0; i < moves.size(); i++)
//            doc.add(moves.get(i));
        doc.add(types);
        doc.add(moves);

        return doc;
    }

    /**
     * Adds a pokemon object to the index after creating a document out of it
     * @param pokemon - pokemon object that needs to be added to the index
     * @throws IOException
     */
    public void addPokemon(Pokemon pokemon) throws IOException {
        Document doc = makeDocument(pokemon);
        indexWriter.addDocument(doc);
    }

    /**
     * Adds all pokemon as described by the number of pokemon data member
     * Makes socuments out of these pokemon objects and adds it to the index
     * @throws IOException
     */
    public void addPokemon() throws IOException{
        Pokemon[] pokemons = Data.POKEMONS;
        for(Pokemon pokemon: pokemons) {
            Document document = makeDocument(pokemon);
            indexWriter.addDocument(document);
        }
    }

    /**
     * Closes the IndexWriter object
     * @throws IOException
     */
    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }

    /**
     * Clears the index of all documents
     * @throws IOException
     */
    public void clearIndex() throws IOException{
        if(indexWriter != null) {
            indexWriter.deleteAll();
        }
    }

    /**
     * Build Index function
     * @param clearAndBuildFlag - if this flag is enabled, all existing documents in the index are deleted before indexing is done
     * @throws IOException
     */
    public void buildIndexes(boolean clearAndBuildFlag)throws IOException {
        if(clearAndBuildFlag) {
            clearIndex();
        }
        this.addPokemon();
        this.closeIndexWriter();
    }

    public void deleteIndexes() throws IOException {
        File indexDirectory = new File(indexDirectoryPath);
        String files[] = indexDirectory.list();

        for(String file : files)
            delete(Paths.get(indexDirectory+"/"+file));

        delete(Paths.get(indexDirectoryPath));
    }
}
