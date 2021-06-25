package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import edu.uci.ics.texera.dataflow.regexmatcher.GramBooleanQuery.QueryOp;

/**
 * @author Zuozhi Wang
 * @author Shuying Lai
 *
 */

public class TranslatorUtils {

    static final int MAX_EXACT_SIZE = 7;

    /**
     * Prefix and suffix sets are limited to maxSet strings. If they get too
     * big, "simplify" will replace groups of strings sharing a common leading
     * prefix (or trailing suffix) with that common prefix (or suffix). It is
     * useful for maxSet to be at least 2³ = 8 so that we can exactly represent
     * a case-insensitive abc by the set {abc, abC, aBc, aBC, Abc, AbC, ABc,
     * ABC}.
     */
    static final int MAX_SET_SIZE = 20;

    static final int DEFAULT_GRAM_LENGTH = 3;
    static int GRAM_LENGTH = DEFAULT_GRAM_LENGTH;

    /**
     * This function interface provides a method to fold (concat / alternate)
     * two {@code RegexInfo} objects.
     * 
     * @author laishuying
     *
     */
    @FunctionalInterface
    static interface IFold {
        RegexInfo foldFunc(RegexInfo x, RegexInfo y);
    }

    /**
     * This function interface determines whether a given string contains a
     * given affix (prefix/suffix). If true and they are both in prefix/suffix
     * set, we could remove this string.
     */
    @FunctionalInterface
    static interface IContain {
        boolean containFunc(String str, String affix);
    }

    /**
     * This function returns the length of the shortest string in
     * {@code strList}.
     * 
     * @param strList
     * @return minLen
     */
    static int minLenOfString(List<String> strList) {
        int minLen = Integer.MAX_VALUE;

        for (String str : strList) {
            minLen = Math.min(minLen, str.length());
        }

        return minLen;
    }

    private static int compareSuffix(String str1, String str2) {
        String str1Reverse = new StringBuilder(str1).reverse().toString();
        String str2Reverse = new StringBuilder(str2).reverse().toString();
        return str1Reverse.compareTo(str2Reverse);
    }

    /**
     * This function removes duplicates in prefix/suffix list.
     * 
     * @param strList
     * @param isSuffix
     */
    static void removeDuplicateAffix(List<String> strList, boolean isSuffix) {
        HashSet<String> strSet = new HashSet<String>(strList);
        strList.clear();
        strList.addAll(strSet);
        if (isSuffix) {
            Collections.sort(strList, (str1, str2) -> compareSuffix(str1, str2));
        } else {
            Collections.sort(strList, (str1, str2) -> str1.compareTo(str2));
        }
    }

    /**
     * This function ensures that prefix/suffix sets aren't redundant. For
     * example, if we know "ab" is a possible prefix, then it doesn't help at
     * all to know that "abc" is also a possible prefix, so delete "abc".
     * 
     * @param iContain
     * @param strList
     */
    static void removeRedundantAffix(TranslatorUtils.IContain iContain, List<String> strList) {
        if (strList.size() <= 1) {
            return;
        }
        int w = 0;
        for (String str : strList) {
            if (w == 0 || !iContain.containFunc(str, strList.get(w - 1))) {
                strList.set(w, str);
                w++;
            }
        }
        strList = strList.subList(0, w);
    }

    /**
     * This function calculates the cartesian product of two string lists
     * (treated as set), and removes the duplicates in the results. For example,
     * for xList = {"ab", "a"}, yList = {"bc", "c"}, the cartesian product is
     * {"abbc", "abc", "abc", "ac"}. After removing duplicates, {"abbc", "abc",
     * "ac"} is returned
     * 
     * @param xList
     * @param yList
     * @param isSuffix
     * @return
     */
    static List<String> cartesianProduct(List<String> xList, List<String> yList, boolean isSuffix) {
        List<String> product = new ArrayList<String>();
        for (String x : xList) {
            for (String y : yList) {
                product.add(x + y);
            }
        }
        removeDuplicateAffix(product, isSuffix);
        return product;
    }

    /**
     * This function calculates the union of two string lists (treated as set)
     * and removes the duplicates in the results. For example, for xList =
     * {"ab", "cd"}, yList = {"cd", "ef"}, {"ab", "cd", "ef"} is returned.
     * 
     * @param xList
     * @param yList
     * @param isSuffix
     * @return
     */
    static List<String> union(List<String> xList, List<String> yList, boolean isSuffix) {
        List<String> unionList = new ArrayList<String>(xList);

        unionList.addAll(yList);
        TranslatorUtils.removeDuplicateAffix(unionList, isSuffix);

        return unionList;
    }

    /**
     * A list of characters that need to be escaped in Lucene <br>
     * <a href=
     * "http://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters">
     * Special characters in Lucene 5.5.0 </a>
     */
    static List<String> specialLuceneCharacters = Arrays.asList(new String[] { "\\", // "\\"
                                                                                     // itself
                                                                                     // needs
                                                                                     // to
                                                                                     // be
                                                                                     // escaped
                                                                                     // first
                                                                                     // before
                                                                                     // escaping
                                                                                     // other
                                                                                     // characters
            "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", " ", "AND", "OR",
            "NOT" });

    /**
     * This function escapes special characters in Lucene. <br>
     * For example, if an operand is "a)b", Lucene will treat ")" as a special
     * character. This function changes it to "a\)b". <br>
     * 
     * @param query
     */
    static void escapeSpecialCharacters(GramBooleanQuery query) {
        if (query.operator == QueryOp.LEAF) {
            for (String specialChar : specialLuceneCharacters) {
                query.leaf = query.leaf.replace(specialChar, "\\" + specialChar);
            }
        } else {
            for (GramBooleanQuery subQuery : query.subQuerySet) {
                escapeSpecialCharacters(subQuery);
            }
        }

    }

    static void toLowerCase(GramBooleanQuery query) {
        if (query.operator == QueryOp.LEAF) {
            query.leaf = query.leaf.toLowerCase();
        } else {
            for (GramBooleanQuery subQuery : query.subQuerySet) {
                toLowerCase(subQuery);
            }
        }
    }

}
