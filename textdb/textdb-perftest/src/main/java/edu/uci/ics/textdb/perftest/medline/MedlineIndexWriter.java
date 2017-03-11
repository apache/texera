package edu.uci.ics.textdb.perftest.medline;

import java.text.ParseException;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.FileSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.utils.StorageUtils;

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

    public static final Attribute PMID_ATTR = new Attribute(PMID, AttributeType.INTEGER);
    public static final Attribute AFFILIATION_ATTR = new Attribute(AFFILIATION, AttributeType.TEXT);
    public static final Attribute ARTICLE_TITLE_ATTR = new Attribute(ARTICLE_TITLE, AttributeType.TEXT);
    public static final Attribute AUTHORS_ATTR = new Attribute(AUTHORS, AttributeType.TEXT);
    public static final Attribute JOURNAL_ISSUE_ATTR = new Attribute(JOURNAL_ISSUE, AttributeType.STRING);
    public static final Attribute JOURNAL_TITLE_ATTR = new Attribute(JOURNAL_TITLE, AttributeType.TEXT);
    public static final Attribute KEYWORDS_ATTR = new Attribute(KEYWORDS, AttributeType.TEXT);
    public static final Attribute MESH_HEADINGS_ATTR = new Attribute(MESH_HEADINGS, AttributeType.TEXT);
    public static final Attribute ABSTRACT_ATTR = new Attribute(ABSTRACT, AttributeType.TEXT);
    public static final Attribute ZIPF_SCORE_ATTR = new Attribute(ZIPF_SCORE, AttributeType.DOUBLE);

    public static final Attribute[] ATTRIBUTES_MEDLINE = { PMID_ATTR, AFFILIATION_ATTR, ARTICLE_TITLE_ATTR,
            AUTHORS_ATTR, JOURNAL_ISSUE_ATTR, JOURNAL_TITLE_ATTR, KEYWORDS_ATTR, MESH_HEADINGS_ATTR, ABSTRACT_ATTR,
            ZIPF_SCORE_ATTR };

    public static final Schema SCHEMA_MEDLINE = new Schema(ATTRIBUTES_MEDLINE);

    private static Tuple recordToTuple(String record) throws JSONException, ParseException {
        JSONObject json = new JSONObject(record);
        ArrayList<IField> fieldList = new ArrayList<IField>();
        for (Attribute attr : ATTRIBUTES_MEDLINE) {
            fieldList.add(StorageUtils.getField(attr.getAttributeType(), json.get(attr.getAttributeName()).toString()));
        }
        IField[] fieldArray = new IField[fieldList.size()];
        Tuple tuple = new Tuple(SCHEMA_MEDLINE, fieldList.toArray(fieldArray));
        return tuple;
    }

    /**
     * This function generates a plan that reads a file using
     * FileSourceOperator, then writes index to the table using IndexSink.
     * 
     * the table must be pre-created (it must already exist)
     * 
     * @param filePath,
     *            path of the file to be read
     * @param tableName,
     *            table name of the table to be written into (must already exist)
     * @return the plan to write a Medline index
     * @throws TextDBException
     */
    public static Plan getMedlineIndexPlan(String filePath, String tableName) throws TextDBException {
        IndexSink medlineIndexSink = new IndexSink(tableName, false);
        ISourceOperator fileSourceOperator = new FileSourceOperator(filePath, (s -> recordToTuple(s)),
                RelationManager.getRelationManager().getTableSchema(tableName));
        medlineIndexSink.setInputOperator(fileSourceOperator);

        Plan writeIndexPlan = new Plan(medlineIndexSink);

        return writeIndexPlan;
    }

}
