package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

/*
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 */

public class RegexToTrigram {	

	/**
	 * Translate a regular expression to an object of TrigramBooleanQeruy
	 * @param regex 
	 * @return TrigramBooleanQuery
	 */
	public static TrigramBooleanQuery translate(String regex) {
	    Regexp re = Parser.parse(regex, RE2.PERL);
	    re = Simplify.simplify(re);
	    RegexInfo regexInfo = analyze(re);
	    simplify(regexInfo);
	    return regexInfo.match;
	}
	
	
	private static RegexInfo analyze(Regexp re) {
		RegexInfo regexInfo = new RegexInfo();
		switch (re.op) {
		case ALTERNATE:
			break;
		case ANY_CHAR:
			break;
		case ANY_CHAR_NOT_NL:
			break;
		case BEGIN_LINE:
			break;
		case BEGIN_TEXT:
			break;
		case CAPTURE:
			break;
		case CHAR_CLASS:
			break;
		case CONCAT:
			break;
		case EMPTY_MATCH:
			break;
		case END_LINE:
			break;
		case END_TEXT:
			break;
		case LEFT_PAREN:
			break;
		case LITERAL:
			break;
		case NO_MATCH:
			break;
		case NO_WORD_BOUNDARY:
			break;
		case PLUS:
			break;
		case QUEST:
			break;
		case REPEAT:
			break;
		case STAR:
			break;
		case VERTICAL_BAR:
			break;
		case WORD_BOUNDARY:
			break;
		default:
			break;
		}
		
		return regexInfo;
	}
	
	private static void simplify(RegexInfo regexInfo) {
		
	}
}
