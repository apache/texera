
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

/**
 * @author Sudeep [inkudo]
 * 
 */
public class DictionaryMatcher implements IOperator {

    private IOperator operator;
    private IDictionary dictionary;
    private String dictionaryValue;
    private int positionIndex; // next position in the field to be checked.
    private int fieldIndex; // Index of the next field to be checked.
    private int spanIndexValue; // Starting position of the matched dictionary
                                // string
    private String fieldName;
    private ITuple dataTuple;
    private List<IField> fields;
    private Schema schema;

    public DictionaryMatcher(IDictionary dictionary, IOperator operator) {
        this.operator = operator;
        this.dictionary = dictionary;
    }

    /**
     * @about Opens dictionary matcher. Initializes positionIndex and fieldIndex
     *        Gets first sourceoperator tuple and dictionary value.
     */
    @Override
    public void open() throws Exception {
        try {
            positionIndex = 0;
            fieldIndex = 0;
            operator.open();
            dictionaryValue = dictionary.getNextValue();
            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();
            schema = dataTuple.getSchema();

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan based search, gets the dictionary
     *        value and scans all the documents for matches
     * 
     * @overview Loop through the dictionary entries. For each dictionary entry,
     *           loop through the tuples in the operator. For each tuple, loop
     *           through all the fields. For each field, loop through all the
     *           matches.
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (fieldIndex < fields.size()) {
            IField dataField = dataTuple.getField(fieldIndex);
            if (dataField instanceof StringField) {
                String fieldValue = ((StringField) dataField).getValue();
                fieldValue = fieldValue.toLowerCase();
                dictionaryValue = dictionaryValue.toLowerCase();

                // Get position of dict value in the field.
                if ((spanIndexValue = fieldValue.indexOf(dictionaryValue, positionIndex)) != -1) {

                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexValue + dictionaryValue.length();

                    Attribute attribute = schema.getAttributes().get(fieldIndex);
                    fieldName = attribute.getFieldName();

                    return getSpanTuple();

                } else {
                    // Increment the fieldIndex and call getNextTuple to search
                    // in next field
                    fieldIndex++;
                    positionIndex = 0;
                    return getNextTuple();
                }
            } else {
                // If fieldType is not String. Presently only supporting string
                // type in dictionary
                fieldIndex++;
                positionIndex = 0;
                return getNextTuple();
            }

        }
        // Get the next document
        else if ((dataTuple = operator.getNextTuple()) != null) {
            fieldIndex = 0;
            positionIndex = 0;
            fields = dataTuple.getFields();
            return getNextTuple();

        }
        // Get the next dictionary value
        else if ((dictionaryValue = dictionary.getNextValue()) != null) {
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            fieldIndex = 0;
            positionIndex = 0;

            operator.close();
            operator.open();
            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();
            return getNextTuple();
        }

        return null;
    }

    /**
     * @about Modifies schema, fields and creates a new span tuple
     */
    private ITuple getSpanTuple() {
        List<Attribute> attributesCopy = new ArrayList<>(schema.getAttributes());
        attributesCopy.add(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_KEY_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_BEGIN_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_END_ATTRIBUTE);

        List<IField> fieldListDuplicate = new ArrayList<>(fields);
        fieldListDuplicate.add(new StringField(fieldName));
        fieldListDuplicate.add(new StringField(dictionaryValue));
        fieldListDuplicate.add(new IntegerField(spanIndexValue));
        fieldListDuplicate.add(new IntegerField(positionIndex - 1));

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(new Schema(attributesCopy), fieldsDuplicate);
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            operator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
