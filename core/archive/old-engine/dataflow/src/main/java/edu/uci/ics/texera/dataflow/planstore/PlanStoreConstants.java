package edu.uci.ics.texera.dataflow.planstore;

import java.nio.file.Path;
import java.util.regex.Pattern;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.utils.Utils;

/**
 * Variables used in PlanStore.java.
 *
 * @author Adrian Seungjin Lee
 * @author Kishore Narendran
 */
public class PlanStoreConstants {
    public static final String TABLE_NAME = "plan";

    public static final Pattern VALID_PLAN_NAME = Pattern.compile("^[a-zA-Z0-9\\-_]{1,}$");

    public static final Path INDEX_DIR = Utils.getTexeraHomePath().resolve("user-resources").resolve("plans");

    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String LOGICAL_PLAN_JSON = "logicalPlan";

    public static final Attribute NAME_ATTR = new Attribute(NAME, AttributeType.STRING);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, AttributeType.STRING);
    public static final Attribute LOGICAL_PLAN_JSON_ATTR = new Attribute(LOGICAL_PLAN_JSON, AttributeType.STRING);

    public static final Attribute[] ATTRIBUTES_PLAN = {NAME_ATTR, DESCRIPTION_ATTR, LOGICAL_PLAN_JSON_ATTR};
    public static final Schema SCHEMA_PLAN = new Schema(ATTRIBUTES_PLAN);
}
