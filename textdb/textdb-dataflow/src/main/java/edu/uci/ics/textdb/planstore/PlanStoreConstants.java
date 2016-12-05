package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

import java.util.regex.Pattern;

/**
 * Variables used in PlanStore.java.
 *
 * @author Adrian Seungjin Lee
 */
public class PlanStoreConstants {
    public static final String TABLE_NAME = "plan";

    public static final Pattern VALID_PLAN_NAME = Pattern.compile("^[a-zA-Z0-9\\-_]{1,}$");

    public static final String INDEX_DIR = "../plan";
    public static final String FILES_DIR = "../plan_files";
    public static final String FILE_SUFFIX = ".ser";

    public static final String NAME = "name";
    public static final String DESCRIPTION = "desc";
    public static final String FILE_PATH = "filePath";

    public static final Attribute NAME_ATTR = new Attribute(NAME, FieldType.STRING);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, FieldType.STRING);
    public static final Attribute FILE_PATH_ATTR = new Attribute(FILE_PATH, FieldType.STRING);

    public static final Attribute[] ATTRIBUTES_PLAN = {NAME_ATTR, DESCRIPTION_ATTR, FILE_PATH_ATTR};
    public static final Schema SCHEMA_PLAN = new Schema(ATTRIBUTES_PLAN);
}
