package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/*
 * Test data of some text contents.
 * @author Zuozhi Wang
 * @author Qing Tang
 */
public class RegexTestConstantsText {
    // Sample test data of some random text
    public static final String CONTENT = "content";

    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, AttributeType.TEXT);

    public static final Attribute[] ATTRIBUTES_TEXT = { CONTENT_ATTR };
    public static final Schema SCHEMA_TEXT = new Schema(ATTRIBUTES_TEXT);

    private static Tuple getTextTuple(String content) {
        IField field = new TextField(content);
        Tuple tuple = new Tuple(SCHEMA_TEXT, field);
        return tuple;
    }

    public static List<Tuple> getSampleTextTuples() {
        List<Tuple> textTuples = new ArrayList<>();
        textTuples.add(getTextTuple("This testcase is for testing regex that can be translated by the translator"));
        textTuples.add(getTextTuple(
                "Translator is effective for specific regex, but has less effects on general regular expressions"));
        textTuples.add(getTextTuple(
                "Testing is really really important and we need to write a large amount of high quality tests"));
        textTuples.add(getTextTuple("We need to make sure that corner cases are tested"));

        textTuples.add(getTextTuple("The patient will be seen in followup. She will have a followup."));
        textTuples.add(
                getTextTuple("The patient is to follow up with Dr. Smith. I will follow up the patient in two weeks."));
        textTuples.add(getTextTuple("The patient will have a follow-up (or followup) examination."));

        textTuples.add(getTextTuple(
                "Tomato is the edible, often red berry-type fruit of Solanum lycopersicum, commonly known as a tomato plant."));
        textTuples.add(getTextTuple(
                "Potato is a cool-season vegetable that ranks with wheat and rice as one of the most important staple crops in the human diet around the world."));
        textTuples.add(
                getTextTuple("Most fruit consists primarily of carbohydrate, while avocado is high in healthy fats."));

        textTuples.add(getTextTuple(
                "This regex matches any single character surrounded by brackets, since the brackets are escaped, for example: '[a]' and '[!]'."));
        return textTuples;
    }
}
