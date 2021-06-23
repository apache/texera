package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.util.*;

/**
 * Created by Chang on 8/28/17.
 */
public class TrieNode {

    private final TrieNode root;
    private Map<Character, TrieNode> success = new HashMap<>();
    private TrieNode failure;
    private Set<String> emits = null;

    private TrieNode(boolean isRoot) {
        this.root = isRoot ? this : null;
    }

    public static TrieNode newRootNode() {
        return new TrieNode(true);
    }

    public TrieNode getNextTrieNode(Character c) {
        TrieNode node = this.success.get(c);
        if (node == null && this.root != null) {
            return this.root;
        }
        return node;
    }

    public TrieNode addTrieNode(Character c) {
        TrieNode node = this.success.get(c);
        if (node == null) {
            node = new TrieNode(false);
            this.success.put(c, node);
        }
        return node;
    }

    public void addEmit(String keyword) {
        if (keyword == null || keyword.isEmpty()) return;
        if (this.emits == null) {
            this.emits = new HashSet<>();
        }
        this.emits.add(keyword);
    }

    public void addEmits(List<String> keywordList) {
        if (keywordList == null || keywordList.isEmpty()) return;
        for (String keyword : keywordList) {
            addEmit(keyword);
        }
    }

    public void setFailure(TrieNode failure) {
        this.failure = failure;
    }

    public TrieNode getFailure() {
        return this.failure;
    }

    public List<String> getEmits() {
        return this.emits == null ? Collections.<String>emptyList() : new ArrayList<>(this.emits);
    }

    public List<TrieNode> getChildrenNodes() {
        return new ArrayList<TrieNode>(this.success.values());
    }

    public List<Character> getTransactions() {
        return new ArrayList<Character>(this.success.keySet());
    }

}