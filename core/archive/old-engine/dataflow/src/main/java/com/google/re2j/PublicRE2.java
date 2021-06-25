package com.google.re2j;

/**
 * Wrapper class of re2j.RE2.<!-- --> This class includes regex parse flags.<!--
 * --> <br>
 * It also explains the purpose of wrapper classes in this package. <br>
 * 
 * <a href='github.com/google/re2j'>RE2J</a> is a Java port of RE2, a regular
 * expression engine that runs in linear time. <br>
 * See <a href='github.com/google/re2/wiki/Syntax'>RE2 syntax</a> for syntax
 * accepted by RE2. <br>
 * <p>
 * We use RE2J to parse a regular expression. <br>
 * To get its abstract syntax tree, we need to access RE2J's non-public member
 * variables. RE2J's varaibles are declared at package-level, which means we
 * need to be in the same package to access them. <br>
 * See <a href=
 * 'http://docs.oracle.com/javase/tutorial/java/javaOO/accesscontrol.html'>Java
 * Access Control</a> for different Java access levels. <br>
 * <br>
 * We considered various solutions, including copying all RE2J's source code
 * into our codebase, having our modified fork of RE2J, or using Java reflection
 * to change access level at runtime. However, to keep our codebase clean, we
 * don't want to copy or modify RE2J's code. <br>
 * <br>
 * The following solution allows us to access package-level variables. Since we
 * need to be in the same package, we name our package to be
 * <b>"com.google.re2j"</b> too. Same package name means the same package to
 * Java. <br>
 * 
 * In the com.google.re2j pacakge, we created these wrapper classes to expose
 * what we need to the public. <br>
 * 
 * 
 * </p>
 * 
 * @author Zuozhi Wang
 *
 */
public class PublicRE2 {

    // A flag that tells RE2J to use PERL syntax
    public static final int PERL = RE2.PERL;

    // A flag that tells fold case during matching (case-insensitive).
    public static final int FOLD_CASE = RE2.FOLD_CASE;

}
