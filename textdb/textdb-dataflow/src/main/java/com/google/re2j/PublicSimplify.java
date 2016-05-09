package com.google.re2j;

public class PublicSimplify{
	
	public static PublicRegexp simplify(Regexp re) {
		return PublicRegexp.deepCopy(Simplify.simplify(re));
	}

}
