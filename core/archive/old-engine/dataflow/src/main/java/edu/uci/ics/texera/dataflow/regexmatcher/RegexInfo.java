package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 *         RegexInfo for translating regex to an n-gram boolean query. <br>
 *         see <a href=
 *         'https://swtch.com/~rsc/regexp/regexp4.html'>https://swtch.com/~rsc/regexp/regexp4.html</a>
 *         for details. <br>
 */
class RegexInfo {
    boolean emptyable;
    List<String> exact = null;
    List<String> prefix = null;
    List<String> suffix = null;
    GramBooleanQuery match = null;

    /**
     * This initializes RegexInfo: <br>
     * emptyable to false <br>
     * exact, prefix, suffix to empty ArrayList <br>
     * match with operator ANY <br>
     */
    RegexInfo() {
        this(GramBooleanQuery.QueryOp.ANY);
    }

    RegexInfo(GramBooleanQuery.QueryOp operator) {
        emptyable = false;
        exact = new ArrayList<String>();
        prefix = new ArrayList<String>();
        suffix = new ArrayList<String>();
        match = new GramBooleanQuery(operator);
    }

    /**
     * @return RegexInfo describing a regex that matches NO string This function
     *         shouldn't be called unless something goes wrong. It is used to
     *         handle error cases.
     */
    static RegexInfo matchNone() {
        RegexInfo regexInfo = new RegexInfo(GramBooleanQuery.QueryOp.NONE);
        return regexInfo;
    }

    /**
     * 
     * @return RegexInfo describing a regex that matches ANY string
     */
    static RegexInfo matchAny() {
        RegexInfo regexInfo = new RegexInfo(GramBooleanQuery.QueryOp.ANY);
        regexInfo.emptyable = true;
        regexInfo.prefix.add("");
        regexInfo.suffix.add("");
        return regexInfo;
    }

    /**
     * 
     * @return RegexInfo describing a regex that matches an EMPTY string
     */
    static RegexInfo emptyString() {
        RegexInfo regexInfo = new RegexInfo(GramBooleanQuery.QueryOp.ANY);
        regexInfo.emptyable = true;
        regexInfo.exact.add("");
        return regexInfo;
    }

    /**
     * @return RegexInfo describing a regex that matches ANY SINGLE character
     *         For anyChar, prefix, suffix, and exact are null (unknown),
     *         because we don't know the exact character.
     */
    static RegexInfo anyChar() {
        RegexInfo regexInfo = new RegexInfo(GramBooleanQuery.QueryOp.ANY);
        regexInfo.emptyable = false;
        return regexInfo;
    }

    /**
     * This function simplifies the regexInfo. <br>
     * If the "exact" set gets too large, it will add "exact" to the query tree,
     * and move relevant pieces into prefix and suffix. <br>
     * 
     * Simplification is performed under three circumstances. <br>
     * 1, the size of "exact" set is larger than a threshold. <br>
     * 2, the minimum length of strings in "exact" is greater than or equal to
     * {@code gramLength+1}, if we do not set {@code force = true}. <br>
     * 3, the minimum length of strings in "exact" is greater than or equal to
     * {@code gramLength}, if we set {@code force = true}. <br>
     * 
     * For example, if {@code gramLength} = 3, MAX_EXACT_SIZE = 5. 1. "exact" =
     * {"ab", "bc", "cd", "de", "ef", "fg"} will be simplified because its size
     * is greater than 5, no matter {@code force} is true or false. 2. "exact" =
     * {"abcd", "efghj"} will be simplified, no matter {@code force} is true or
     * false, because its shortest string's length, 4, is greater than or equal
     * to gramLength+1, 4. 3. "exact" = {"abc", "efg"} will simplified only if
     * {@code force} if true.
     * 
     * @param force
     */
    RegexInfo simplify(boolean force) {
        TranslatorUtils.removeDuplicateAffix(exact, false);
        int gramLength = TranslatorUtils.GRAM_LENGTH;

        if (exact.size() > TranslatorUtils.MAX_EXACT_SIZE
                || (TranslatorUtils.minLenOfString(exact) >= gramLength && force)
                || TranslatorUtils.minLenOfString(exact) >= gramLength + 1) {
            // Add exact to match (query tree)
            // Transfer information from exact to prefix and suffix
            match = GramBooleanQuery.combine(match, exact);
            for (String str : exact) {
                if (str.length() < gramLength) {
                    prefix.add(str);
                    suffix.add(str);
                } else {
                    prefix.add(str.substring(0, gramLength));
                    suffix.add(str.substring(str.length() - gramLength, str.length()));
                }
            }
            exact.clear();
        }

        // Add information in prefix and suffix to match
        if (exact.isEmpty()) {
            simplifyAffix(prefix, false);
            simplifyAffix(suffix, true);
        }

        /**
         * Simplification of the query tree ('match') happens at the end of every step of the algorithm.
         */
        GramBooleanQuery dnf = GramBooleanQuery.toDNF(match);
        match = GramBooleanQuery.simplifyDNF(dnf);
        return this;
    }

    /**
     * simplifyAffix reduces the size of the given set (either prefix or
     * suffix). If the set gets too big, it moves information in prefix/suffix
     * into "match" query.
     * 
     * @param strList
     * @param isSuffix
     *            indicates if given string list is suffix list or not
     */
    void simplifyAffix(List<String> strList, boolean isSuffix) {
        TranslatorUtils.removeDuplicateAffix(strList, isSuffix);
        int gramLength = TranslatorUtils.GRAM_LENGTH;

        // Add the current prefix/suffix set to "match" query.
        match = GramBooleanQuery.combine(match, strList);

        // This loop reduces the length of prefix/suffix. It cuts all
        // strings longer than {@code gramLength}, and continues to cut strings
        // until the size of the list is below a threshold.
        // It cuts a prefix (suffix) string by only retaining the first (last) n
        // characters of it
        // For example, for a prefix string "abcd", after cutting, it becomes
        // "abc" if n = 3, "ab" if n = 2.
        // For a suffix string "abcd", after cutting, it becomes "bcd" if n = 3,
        // "cd" if n = 2;
        for (int n = gramLength; n == gramLength || strList.size() > TranslatorUtils.MAX_SET_SIZE; n--) {
            // replace set by strings of length n-1
            int w = 0;
            for (String str : strList) {
                if (str.length() > n) {
                    if (!isSuffix) { // prefix
                        str = str.substring(0, n);
                    } else { // suffix
                        str = str.substring(str.length() - n, str.length());
                    }
                }
                if (w == 0 || strList.get(w - 1) != str) {
                    strList.set(w, str);
                    w++;
                }
            }
            strList = strList.subList(0, w);
            TranslatorUtils.removeDuplicateAffix(strList, isSuffix);
        }

        if (!isSuffix) {
            TranslatorUtils.removeRedundantAffix((s, prefix) -> s.startsWith(prefix), strList);
        } else {
            TranslatorUtils.removeRedundantAffix((s, suffix) -> s.endsWith(suffix), strList);
        }

    }

}