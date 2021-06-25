package edu.uci.ics.texera.dataflow.dictionarymatcher;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Chang on 10/4/17.
 */
public class ACTrieTest {

    /**
     * Test substring matching in the middle of one word.
      * @throws Exception
     */
    @Test
    public void testACTrieSinglePath() throws Exception {
        ACTrie trie = new ACTrie();
        trie.addKeywords(new ArrayList<>(Arrays.asList("hers")));
        trie.constructFailureTransactions();
        String text = "h, he, her, hers, herself";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 2);
    }

    /**
     * Test dense trie failure transitions.
     * @throws Exception
     */
    @Test
    public void testACTrieMultiple() throws Exception {
        ACTrie trie = new ACTrie();
        trie.addKeywords(new ArrayList<>(Arrays.asList("he","hers", "his", "she")));
        trie.constructFailureTransactions();
        String text = "ahishers";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 4);
    }

    /**
     * Test matching case-insensitively.
     * @throws Exception
     */
    @Test
    public void testACTrieCaseInsensitive() throws Exception {
        ACTrie trie = new ACTrie();
        trie.setCaseInsensitive(true);
        trie.addKeywords(new ArrayList<>(Arrays.asList("Beta")));
        trie.constructFailureTransactions();
        String text = "Alpha Beta beta Gamma";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 2);
        Assert.assertEquals(exactResults.get(0).getKeyword(), "Beta");
    }

    /**
     * Test repeatly matching part of one word.
     * @throws Exception
     */
    @Test
    public void testACTrieSubString() throws Exception {
        ACTrie trie = new ACTrie();
        trie.setCaseInsensitive(true);
        trie.addKeywords(new ArrayList<>(Arrays.asList("begin", "ing", "dark", "ness")));
        trie.constructFailureTransactions();
        String text = "At the beginning, there is always darkness. But darkness is only at the beginning.";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 8);
    }

    /**
     * Test matching chinese character.
     * @throws Exception
     */
    @Test
    public void testACTrieChineseRepeat() throws Exception {
        ACTrie trie = new ACTrie();
        trie.addKeywords(new ArrayList<>(Arrays.asList("去", "坐", "吃","北京")));
        trie.constructFailureTransactions();
        String text = "去北京，坐在北京天安门，吃北京烤鸭";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 6);
    }

    /**
     * Test matching multiple matching through failure transitions.
     * @throws Exception
     */

    @Test
    public void testACTrieChineseMultiple() throws Exception {
        ACTrie trie = new ACTrie();
        trie.addKeywords(new ArrayList<>(Arrays.asList("太阳", "阳光", "光照","照射", "树枝", "枝叶", "叶子")));
        trie.constructFailureTransactions();
        String text = "太阳光照射树枝叶子";
        List<ACTrie.Emit> exactResults = trie.parseText(text);
        Assert.assertTrue(exactResults.size() == 7);
    }


}
