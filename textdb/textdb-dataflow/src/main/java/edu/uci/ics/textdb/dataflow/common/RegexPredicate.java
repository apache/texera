package edu.uci.ics.textdb.dataflow.common;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

/**
 * This class is responsible for calculating regex match. <br>
 * It uses RegexToGramQueryTranslator to translate a regex to a query in lucene. <br>
 * 
 * @author Zuozhi Wang
 * @author Shuying Lai
 * @author sandeepreddy602
 *
 */
public class RegexPredicate implements IPredicate {

	private String regex;
	private List<String> fieldNameList;
	private List<Attribute> attributeList;
	private Analyzer luceneAnalyzer;
	private IDataStore dataStore;

	public RegexPredicate(String regex, IDataStore dataStore, List<Attribute> attributeList, Analyzer analyzer) {
		this.regex = regex;
		this.luceneAnalyzer = analyzer;
		this.dataStore = dataStore;
		this.attributeList = attributeList;
		this.fieldNameList = attributeList.stream()
				.filter(attr -> (attr.getFieldType() == FieldType.TEXT || attr.getFieldType() == FieldType.STRING))
				.map(attr -> attr.getFieldName()).collect(Collectors.toList());
	}

	public String getRegex() {
		return regex;
	}

	public Analyzer getLuceneAnalyzer() {
		return this.luceneAnalyzer;
	}

	public IDataStore getDataStore() {
		return this.dataStore;
	}

	public List<String> getFieldNameList() {
		return this.fieldNameList;
	}

	public List<Attribute> getAttributeList() {
		return attributeList;
	}

}
