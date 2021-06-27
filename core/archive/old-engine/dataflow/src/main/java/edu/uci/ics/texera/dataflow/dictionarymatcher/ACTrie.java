package edu.uci.ics.texera.dataflow.dictionarymatcher;

/**
 * Created by Chang on 9/23/17.
 */

import java.util.*;

/**
 * This is to implement the aho-corasick algorithm to build automaton
 * for all dictionary entries with a prefix trie and failure transactions.
 * Wiki page link: https://github.com/Texera/texera/wiki/Aho-Corasick-String-Matching-Algorithm
 * Created by Chang on 8/29/17.
 */
public class ACTrie {
    private final TrieNode rootNode;
    private boolean caseInsensitive = false;

    public ACTrie() {
        this.rootNode = TrieNode.newRootNode();
    }

    private void addKeyword(String keyword) {
        if (keyword == null || keyword.length() == 0) return;
        String originalWord = keyword;
        if (caseInsensitive) {
            keyword = keyword.toLowerCase();
        }
        TrieNode currentNode = this.rootNode;
        for (Character character : keyword.toCharArray()) {
            currentNode = currentNode.addTrieNode(character);
        }
        currentNode.addEmit(originalWord);
    }

    public void addKeywords(List<String> keywordList) {
        if (keywordList == null || keywordList.isEmpty()) {
            return;
        }
        for (String keyword : keywordList) {
            addKeyword(keyword);
        }
    }

    /**
     * Run a BFS on the constructed prefix trie to setup links between
     * failed matching node to its longest common suffix on other branches.
     */
    public void constructFailureTransactions() {
        Deque<TrieNode> queue = new ArrayDeque<>();
        for (TrieNode node : this.rootNode.getChildrenNodes()) {
            node.setFailure(this.rootNode);
            queue.add(node);
        }
        while (!queue.isEmpty()) {
            TrieNode currentNode = queue.poll();
            for (Character character : currentNode.getTransactions()) {
                TrieNode nextNode = currentNode.getNextTrieNode(character);
                if (nextNode != null) {
                    queue.add(nextNode);
                    TrieNode failureNode = currentNode.getFailure();
                    while (failureNode.getNextTrieNode(character) == null) {
                        failureNode = failureNode.getFailure();
                    }
                    TrieNode newFailureNode = failureNode.getNextTrieNode(character);
                    nextNode.setFailure(newFailureNode);
                    nextNode.addEmits(newFailureNode.getEmits());
                }
            }
        }
    }

    /**
     * Traverse the input text and firstly try to follow the success transactions to its child node.
     * If it doesn't exist, turn to follow the failure transactions. When reaching a node with non-empty
     * output keywords, add them along with the start and end positions into resultList.
     *
     * @param text
     * @return
     */
    public List<Emit> parseText(String text) {
        List<Emit> resultList = new ArrayList<>();
        if (text == null || text.isEmpty()) return resultList;

        if (caseInsensitive) {
            text = text.toLowerCase();
        }
        TrieNode currentNode = this.rootNode;
        char[] textArray = text.toCharArray();
        for (int i = 0; i < textArray.length; i++) {
            Character character = textArray[i];
            currentNode = getNextTransition(character, currentNode);
            if (!currentNode.getEmits().isEmpty()) {
                resultList.addAll(storeEmits(currentNode, i));
            }
        }
        return resultList;
    }

    private TrieNode getNextTransition(Character c, TrieNode node) {
        TrieNode nextNode = node.getNextTrieNode(c);
        while (nextNode == null) {
            node = node.getFailure();
            nextNode = node.getNextTrieNode(c);
        }
        return nextNode;
    }

    private List<Emit> storeEmits(TrieNode node, int position) {
        List<Emit> resultList = new ArrayList<>();
        for (String matchedKeyword : node.getEmits()) {
            resultList.add(new Emit(position - matchedKeyword.length() + 1, position + 1, matchedKeyword));
        }
        return resultList;
    }

    public void setCaseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
    }

    public static class Emit {
        private int start;
        private int end;
        private String keyword;

        public Emit(int start, int end, String keyword) {
            this.start = start;
            this.end = end;
            this.keyword = keyword;

        }

        public int getStart() {
            return this.start;
        }

        public int getEnd() {
            return this.end;
        }

        public String getKeyword() {
            return this.keyword;
        }
    }
}

