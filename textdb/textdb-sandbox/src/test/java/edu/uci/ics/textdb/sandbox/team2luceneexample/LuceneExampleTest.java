package edu.uci.ics.textdb.sandbox.team2luceneexample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by kishorenarendran on 10/04/16.
 */
public class LuceneExampleTest {

    private static Pokedexer pokedexer;
    private static int numberOfPokemon = 4;
    private static String searchFieldName = LuceneConstants.TYPES_FIELD;
    private static int maxResults = numberOfPokemon;
    private static String allResultsSearchQuery = "*:*";
    private static String termSearchQuery = "grass";

    @BeforeClass
    public static void buildIndex() throws IOException{
        pokedexer = new Pokedexer(LuceneConstants.INDEX, numberOfPokemon);
        pokedexer.buildIndexes(true);
    }

    @Test
    public void testIndex() {
        assertNotNull(pokedexer);
    }

    @Test
    public void testAllSearch() throws IOException, ParseException{
        PokemonSearcher pokemonSearcher = new PokemonSearcher(searchFieldName);
        Document[] documents = pokemonSearcher.performSearch(allResultsSearchQuery, maxResults);
        assertEquals(documents.length, maxResults);
    }

    @Test
    public void testMultiTerm() throws IOException, ParseException
    {
        PokemonSearcher pokemonSearcher = new PokemonSearcher(searchFieldName);
        Document[] documents = pokemonSearcher.performSearch(termSearchQuery, maxResults);
        assertEquals(documents.length, 1);
    }

    @AfterClass
    public static void deleteIndex() throws IOException {
        pokedexer.deleteIndexes();
    }
}
