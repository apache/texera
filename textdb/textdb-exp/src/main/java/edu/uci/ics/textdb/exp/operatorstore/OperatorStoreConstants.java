package edu.uci.ics.textdb.exp.operatorstore;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

import java.util.regex.Pattern;

/**
 * Variables to be used in the OperatorStore.java
 *
 * @author Kishore Narendran
 */
public class OperatorStoreConstants {
    public static final String TABLE_NAME = "operator";

    public static final Pattern VALID_OPERATOR_NAME = Pattern.compile("^[a-zA-Z]{1,}$");

    public static final String INDEX_DIR = "../operators";

    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String PREDICATE_BEAN = "predicateBean";
    public static final String OPERATOR_CONFIG_FILE = "operatorConfigFile";

    public static final Attribute NAME_ATTR = new Attribute(NAME, AttributeType.STRING);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, AttributeType.STRING);
    public static final Attribute PREDICATE_BEAN_ATTR = new Attribute(PREDICATE_BEAN, AttributeType.STRING);
    public static final Attribute OPERATOR_CONFIG_FILE_ATTR = new Attribute(OPERATOR_CONFIG_FILE, AttributeType.STRING);

    public static final Attribute[] ATTRIBUTES_OPERATOR = {NAME_ATTR, DESCRIPTION_ATTR, PREDICATE_BEAN_ATTR,
            OPERATOR_CONFIG_FILE_ATTR};
    public static final Schema SCHEMA_OPERATOR = new Schema(ATTRIBUTES_OPERATOR);
}
