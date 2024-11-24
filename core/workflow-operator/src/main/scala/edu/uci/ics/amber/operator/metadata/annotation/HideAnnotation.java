package edu.uci.ics.amber.operator.metadata.annotation;

/* For every hide annotation, you specify on a formly field three things:
    the target (field name of dependent comparison),
    the hide type (specified by Type),
    and the expected value (specified as a string).

    This information is passed to the frontend and hidden.
    Here's how you specify an example hide, hiding someOtherFieldName when someFieldName == 3:
    @JsonSchemaInject(strings = {
            @JsonSchemaString(path = HideAnnotation.hideTarget, value = "someFieldName"),
            @JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
            @JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "3")
    })
    public Integer someOtherFieldName;

    public Integer someFieldName;
 */
public class HideAnnotation {
    public final static String hideTarget = "hideTarget";
    public final static String hideType = "hideType";
    public final static String hideExpectedValue = "hideExpectedValue";
    public final static String hideOnNull = "hideOnNull";

    /* The types of matching on which a hide occurs. Evaluated at runtime by javascript. */
    public static class Type {
        /* String equality operator is applied to assert hideTarget == hideExpectedValue. */
        public final static String equals = "equals";

        /* Regex matching is applied to assert regex for hideExpectedValue matches hideTarget */
        public final static String regex = "regex";
    }
}