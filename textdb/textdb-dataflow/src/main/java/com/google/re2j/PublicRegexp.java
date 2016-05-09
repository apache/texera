package com.google.re2j;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PublicRegexp extends Regexp {
	
	private PublicRegexp[] publicSubs;

	// shallow copy constructor
	private PublicRegexp(Regexp that) {
		super(that);
	}
	  
	// deep copy
	public static PublicRegexp deepCopy(Regexp re) {
		PublicRegexp publicRegexp = new PublicRegexp(re);
		
		Stream<PublicRegexp> publicSubStream = 
				Arrays.asList(re.subs).stream()
				.map(sub -> PublicRegexp.deepCopy(sub));
		publicRegexp.publicSubs = new PublicRegexp[re.subs.length];
		publicSubStream.collect(Collectors.toList()).toArray(publicRegexp.publicSubs);
		
		return publicRegexp;
	}
	

	public String getOp() {
		return this.op.toString();
	}
	
	public int getFlags() {
		return this.flags;
	}
	
	public PublicRegexp[] getSubs() {
		return this.publicSubs;
	}
	
	public int[] getRunes() {
		return this.runes;
	}
	
	public int getMin() {
		return this.min;
	}
	
	public int getMax() {
		return this.max;
	}
	
	public int getCap() {
		return this.cap;
	}
	
	public String getCapName() {
		return this.name;
	}
	
}
