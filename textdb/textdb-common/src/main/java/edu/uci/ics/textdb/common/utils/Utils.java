package edu.uci.ics.textdb.common.utils;

import java.text.ParseException;
import java.util.Date;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexableField;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

public class Utils {
    public static IField getField(FieldType fieldType, String fieldValue) throws ParseException{
        IField field = null;
        switch (fieldType) {
            case STRING:
                field = new StringField(fieldValue);
                break;
            case INTEGER:
                field = new IntegerField(Integer.parseInt(fieldValue));
                break;
            case DOUBLE:
                field = new DoubleField(Double.parseDouble(fieldValue));
                break;
            case DATE:
                field = new DateField(DateTools.stringToDate(fieldValue));
                break;
            default:
                break;
        }
        return field;
    }

    public static IndexableField getLuceneField(FieldType fieldType,
            String fieldName, Object fieldValue) {
        IndexableField luceneField = null;
        switch(fieldType){
            case STRING:
                luceneField = new org.apache.lucene.document.StringField(
                        fieldName, (String) fieldValue, Store.YES);
                break;
            case INTEGER:
                luceneField = new IntField(
                        fieldName, (Integer) fieldValue, Store.YES);
                break;
            case DOUBLE:
                double value = (Double) fieldValue;
                luceneField = new org.apache.lucene.document.DoubleField(
                        fieldName, value, Store.YES);
                break;
            case DATE:
                String dateString = DateTools.dateToString((Date) fieldValue, Resolution.MILLISECOND);
                luceneField = new org.apache.lucene.document.StringField(fieldName, dateString, Store.YES);
                break;
        }
        return luceneField;
    }
}
