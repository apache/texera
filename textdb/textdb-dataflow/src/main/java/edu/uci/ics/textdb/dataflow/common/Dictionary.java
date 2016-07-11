package edu.uci.ics.textdb.dataflow.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import edu.uci.ics.textdb.api.common.IDictionary;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

    private Iterator<String> iterator;
    private HashMap<String, Double> wordFrequencyMap;

    public Dictionary(Collection<String> dictionaryWords) {
        //Using LinkedHashMap so that getNextValue() returns the words in order.
        this.wordFrequencyMap = new LinkedHashMap<String, Double>();
        for(String word: dictionaryWords) {
            wordFrequencyMap.put(word, 0.0);
        }
    }

    public Dictionary(String wordBaseSourceFilePath) throws IOException {

        BufferedReader dictionaryReader = null;
        String line;
        this.wordFrequencyMap = new LinkedHashMap<String, Double>();
        try {
            URL dictionaryURL = getClass().getResource(wordBaseSourceFilePath);
            dictionaryReader = new BufferedReader(new FileReader(dictionaryURL.getPath()));
            while ((line = dictionaryReader.readLine()) != null) {
                String[] lineContents = line.split(",");
                String word = lineContents[0];
                double frequency;
                try {
                    frequency = Double.parseDouble(lineContents[1]);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    frequency = 1;
                }
                wordFrequencyMap.put(word, frequency);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (dictionaryReader != null) {
                try {
                    dictionaryReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Gets next value from the dictionary
     */
    @Override
    public String getNextValue() {
        if(iterator == null){
            iterator = wordFrequencyMap.keySet().iterator();
        }
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public boolean contains(String word) {
        return wordFrequencyMap.containsKey(word);
    }

    public double getFrequency(String word) {
        if(wordFrequencyMap.containsKey(word)) {
            return wordFrequencyMap.get(word);
        } else {
            return -1;
        }
    }
}
