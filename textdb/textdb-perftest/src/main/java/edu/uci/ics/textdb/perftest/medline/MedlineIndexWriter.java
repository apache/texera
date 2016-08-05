package edu.uci.ics.textdb.perftest.medline;

import java.text.ParseException;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.FileSourceOperator;

/*
 * This class defines Medline data schema.
 * It also provides functions to read Medline file 
 * from local machine, parse json format,
 * and return a generated ITuple one by one.
 * 
 * @author Zuozhi Wang
 * @author Jinggang Diao
 */
public class MedlineIndexWriter {

	public static final String PMID = "pmid";
	public static final String AFFILIATION = "affiliation";
	public static final String ARTICLE_TITLE = "article_title";
	public static final String AUTHORS = "authors";
	public static final String JOURNAL_ISSUE = "journal_issue";
	public static final String JOURNAL_TITLE = "journal_title";
	public static final String KEYWORDS = "keywords";
	public static final String MESH_HEADINGS = "mesh_headings";
	public static final String ABSTRACT = "abstract";
	public static final String ZIPF_SCORE = "zipf_score";

	public static final Attribute PMID_ATTR = new Attribute(PMID, FieldType.INTEGER);
	public static final Attribute AFFILIATION_ATTR = new Attribute(AFFILIATION, FieldType.TEXT);
	public static final Attribute ARTICLE_TITLE_ATTR = new Attribute(ARTICLE_TITLE, FieldType.TEXT);
	public static final Attribute AUTHORS_ATTR = new Attribute(AUTHORS, FieldType.TEXT);
	public static final Attribute JOURNAL_ISSUE_ATTR = new Attribute(JOURNAL_ISSUE, FieldType.STRING);
	public static final Attribute JOURNAL_TITLE_ATTR = new Attribute(JOURNAL_TITLE, FieldType.TEXT);
	public static final Attribute KEYWORDS_ATTR = new Attribute(KEYWORDS, FieldType.TEXT);
	public static final Attribute MESH_HEADINGS_ATTR = new Attribute(MESH_HEADINGS, FieldType.TEXT);
	public static final Attribute ABSTRACT_ATTR = new Attribute(ABSTRACT, FieldType.TEXT);
	public static final Attribute ZIPF_SCORE_ATTR = new Attribute(ZIPF_SCORE, FieldType.DOUBLE);

	public static final Attribute[] ATTRIBUTES_MEDLINE = { 
		PMID_ATTR, AFFILIATION_ATTR, ARTICLE_TITLE_ATTR, AUTHORS_ATTR, JOURNAL_ISSUE_ATTR,
		JOURNAL_TITLE_ATTR, KEYWORDS_ATTR, MESH_HEADINGS_ATTR, ABSTRACT_ATTR, ZIPF_SCORE_ATTR };
	
	public static final Schema SCHEMA_MEDLINE = new Schema(ATTRIBUTES_MEDLINE);

	
	private static ITuple recordToTuple(String record) throws JSONException, ParseException {
		JSONObject json = new JSONObject(record);
		ArrayList<IField> fieldList = new ArrayList<IField>();
		for (Attribute attr: ATTRIBUTES_MEDLINE) {
			fieldList.add(Utils.getField(attr.getFieldType(), json.get(attr.getFieldName()).toString()) );
		}
		IField[] fieldArray = new IField[fieldList.size()];
		ITuple tuple = new DataTuple(SCHEMA_MEDLINE, fieldList.toArray(fieldArray));
		return tuple;
	}
	
	/**
	 * This function generates a plan that reads a file using FileSourceOperator, then writes index using IndexSink.
	 * @param filePath, path of the file to be read
	 * @param dataStore, dataStore of the index to be written into
	 * @param luceneAnalyzer
	 * @return the plan to write a Medline index
	 * @throws Exception
	 */	
	public static Plan getMedlineIndexPlan(String filePath, IDataStore dataStore, Analyzer luceneAnalyzer) throws Exception {
		IndexSink medlineIndexSink = new IndexSink(dataStore.getDataDirectory(), dataStore.getSchema(), luceneAnalyzer);		
		ISourceOperator fileSourceOperator  = new FileSourceOperator(filePath, (s -> recordToTuple(s)), dataStore.getSchema());
		medlineIndexSink.setInputOperator(fileSourceOperator);
	
		Plan writeIndexPlan = new Plan(medlineIndexSink);

		return writeIndexPlan;
	}

}
