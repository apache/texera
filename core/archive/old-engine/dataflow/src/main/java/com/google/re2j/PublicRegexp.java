package com.google.re2j;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Public Wrapper class for re2j.Regexp.<!-- --> This class represents the
 * abstract syntax tree. <br>
 * 
 * <p>
 * For example, <br>
 * regex: "abc", abstract syntax tree:<br>
 * CONCAT <br>
 * --LITERAL a <br>
 * --LITERAL b <br>
 * --LITERAL c <br>
 * </p>
 * 
 * <p>
 * regex: "a*|b", abstract syntax tree: <br>
 * ALTERNATE <br>
 * --STAR <br>
 * ----LITERAL a <br>
 * --LITERAL b <br>
 * </p>
 * 
 * <p>
 * regex: "[a-f]{1-3}", abstract syntax tree: <br>
 * REPEAT min:1, max:3 <br>
 * --CHAR_CLASS a-f <br>
 * </p>
 * 
 * @author Zuozhi Wang
 *
 */
public class PublicRegexp extends Regexp {
    /*
     * // Fields originally declared in Regexp. // For detailed explanations
     * please see cooresponding getter methods
     * 
     * // Note that for subexpressions, although original comments // say it's
     * never null, it could still be null.
     * 
     * Op op; // operator int flags; // bitmap of parse flags Regexp[] subs; //
     * subexpressions, if any. Never null. // subs[0] is used as the freelist.
     * 
     * int[] runes; // matched runes, for LITERAL, CHAR_CLASS int min, max; //
     * min, max for REPEAT int cap; // capturing index, for CAPTURE String name;
     * // capturing name, for CAPTURE
     * 
     */

    // publicSubs are an array of subexpressions with type PublicRegexp
    PublicRegexp[] publicSubs;

    /**
     * This calls the shallow copy constructor in Regexp superclass, which only
     * copies reference to subexpressions array. <br>
     */
    private PublicRegexp(Regexp that) {
        super(that);
    }

    /**
     * This performs a deep copy of a Regexp object. Every Regexp Object in
     * subexpression arrary is converted to a PublicRegexp object and put in
     * publicSubs array. <br>
     * This is the only public entry point to construct a PublicRegexp object.
     * <br>
     * 
     * @param re,
     *            a Regexp that needs to be converted to PublicRegexp
     * @return PublicRegexp
     */
    public static PublicRegexp deepCopy(Regexp re) {
        PublicRegexp publicRegexp = new PublicRegexp(re);
        if (re.subs != null) {
            // initialize publicSubs array
            publicRegexp.publicSubs = new PublicRegexp[re.subs.length];
            // map every Regexp sub-expression to a PublicRegexp sub-expression
            Stream<PublicRegexp> publicSubStream = Arrays.asList(re.subs).stream()
                    .map(sub -> PublicRegexp.deepCopy(sub));
            // convert the result PublicRegexp subexpressions to an array
            publicSubStream.collect(Collectors.toList()).toArray(publicRegexp.publicSubs);
        } else {
            publicRegexp.publicSubs = null;
        }
        return publicRegexp;
    }

    /**
     * Enum types of Op (operator), which represents the operator type of
     * current node in abstract syntax tree. <br>
     * This enum is identical to Regex.Op, which is not public. <br>
     * 
     * @author zuozhi
     *
     */
    public enum PublicOp {
        NO_MATCH, // Matches no strings.
        EMPTY_MATCH, // Matches empty string.
        LITERAL, // Matches runes[] sequence
        CHAR_CLASS, // Matches Runes interpreted as range pair list
        ANY_CHAR_NOT_NL, // Matches any character except '\n'
        ANY_CHAR, // Matches any character
        BEGIN_LINE, // Matches empty string at end of line
        END_LINE, // Matches empty string at end of line
        BEGIN_TEXT, // Matches empty string at beginning of text
        END_TEXT, // Matches empty string at end of text
        WORD_BOUNDARY, // Matches word boundary `\b`
        NO_WORD_BOUNDARY, // Matches word non-boundary `\B`
        CAPTURE, // Capturing subexpr with index cap, optional name name
        STAR, // Matches subs[0] zero or more times.
        PLUS, // Matches subs[0] one or more times.
        QUEST, // Matches subs[0] zero or one times.
        REPEAT, // Matches subs[0] [min, max] times; max=-1 => no limit.
        CONCAT, // Matches concatenation of subs[]
        ALTERNATE, // Matches union of subs[]

        // Pseudo ops, used internally by Parser for parsing stack:
        // These shouldn't be in the final parse tree
        LEFT_PAREN, VERTICAL_BAR;
    }

    /**
     * This returns the op's type, {@link PublicOp}, which is equivalent to
     * Regexp.Op. <br>
     * 
     * @return PublicRegex.PublicOp, an enum type representing the operator
     */
    public PublicOp getOp() {
        try {
            return PublicOp.valueOf(this.op.toString());
        } catch (IllegalArgumentException e) {
            return PublicOp.NO_MATCH;
        }

    }

    /**
     * This returns a bitmap of parse flags. <br>
     * 
     * @see PublicRE2 for possible flags
     * @return a bitmap of parse flags
     */
    public int getFlags() {
        return this.flags;
    }

    /**
     * This returns an array of sub-expressions with type PublicRegexp. <br>
     * 
     * @return an array of subexpressions
     */
    public PublicRegexp[] getSubs() {
        return this.publicSubs;
    }

    /**
     * Runes are a sequence of characters. It stores information related to
     * literals and character classes, and has different interpretations for
     * different ops. <br>
     * <p>
     * For example, <br>
     * regex: "[a-z]", runes: [a,z] <br>
     * interpretation: a character class from a to z <br>
     * regex: "[a-cx-z]", runes: [a,c,x,z] <br>
     * interpretation: a character class from a to c, and from x to z <br>
     * regex: "cat", runes [c,a,t] <br>
     * interpretation: a literal "cat" <br>
     * </p>
     * 
     * @return an array of runes
     */
    public int[] getRunes() {
        return this.runes;
    }

    /**
     * Min and Max are used for repetitions numbers. <br>
     * <p>
     * For example, <br>
     * regex: "a{3,5}", min will be 3, and max will be 5 <br>
     * </p>
     * 
     * @return an int indicating minimum number of repetitions
     */
    public int getMin() {
        return this.min;
    }

    /**
     * @see getMin
     * @return an int indicating maxinum number of repetitions
     */
    public int getMax() {
        return this.max;
    }

    /**
     * Cap is the capturing index. Expressions in () become a capture group. The
     * entire regex's capturing index is 0, and other groups' indexes start from
     * 1. <br>
     * <p>
     * For example, <br>
     * regex: "(a)(b)" <br>
     * for "(a)", cap will be 1, for "(b)", cap will be 2 <br>
     * </p>
     * 
     * @return an int indicating capture index
     */
    public int getCap() {
        return this.cap;
    }

    /**
     * Name is capturing group's name (if any). <br>
     * <p>
     * For example, <br>
     * regex: {@literal "(?<name1>a)(?\<name2>b)"} <br>
     * for {@literal "(?\<name1>a)"}, cap name will be name1 <br>
     * for {@literal "(?\<name2>b)"}, cap name will be name2 <br>
     * </p>
     * 
     * @return an int indicating capture index
     */
    public String getCapName() {
        return this.name;
    }

}
