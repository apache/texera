package com.google.re2j;

/**
 * Wrapper class of re2j.Parser.<!-- --> This class parses a regex string to a
 * PublicRegexp object. <br>
 * 
 * @author Zuozhi Wang
 *
 */
public class PublicParser {

    /**
     * This method parses a regex string, and returns a PublicRegexp object,
     * which represents the abstract syntax tree
     * 
     * @param regex,
     *            the regex string to be parsed
     * @param flags,
     *            parse flags, see PublicRE2 for possible flags
     * @return PublicRegexp
     * @throws PatternSyntaxException
     *             if parsing fails
     */
    public static PublicRegexp parse(String regex, int flags) throws PatternSyntaxException {
        return PublicRegexp.deepCopy(Parser.parse(regex, flags));
    }

}
