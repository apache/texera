package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.*;

public class keywordTestConstants {
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

    public static List<Tuple> getSampleMedlineRecord() {

        IField[] fields1 = { new IntegerField(14347980), new TextField(""),
                new TextField("CHRONIC MENINGOCOCCEMIA; EPIDEMIOLOGY, DIAGNOSIS AND TREATMENT."),
                new TextField("D S BLOOM"), new StringField("103 Aug, 1965"), new TextField("California medicine"),
                new TextField("DRUG THERAPY, MENINGOCOCCAL INFECTIONS, PENICILLIN G, SULFONAMIDES"),
                new TextField("Drug Therapy, Meningococcal Infections, Penicillin G, Sulfonamides"),
                new TextField(
                        "This report describes four cases of chronic meningococcemia with the characteristic manifestations of recurrent episodes of "
                                + "fever, chills, night sweats, headache and anorexia, associated with skin rash and arthralgias. The diagnosis was established in all instances by blood culture. Administration "
                                + "of sulfonamides in three cases and penicillin in the fourth resulted in prompt recovery. The recent finding of a strain of sulfonamide-resistant meningococci, however, indicates "
                                + "that antibiotic-sensitivity tests should be carried out in all cases of meningococcal disease. While waiting for the results of such tests to be reported, the clinician should "
                                + "initiate treatment with large doses of a sulfonamide and penicillin in combination."),
                new DoubleField(0.664347980) };

        IField[] fields2 = { new IntegerField(17832788), new TextField(""), new TextField("Cosmic X-ray Sources."),
                new TextField("S Bowyer, E T Byram, T A Chubb, H Friedman"), new StringField("147-3656 Jan 22, 1965"),
                new TextField("Science (New York, N.Y.)"), new TextField(""), new TextField(""),
                new TextField(
                        "Eight new sources of cosmic x-rays were detected by two Aerobee surveys in 1964. One source, from Sagittarius, is close to the galactic center, and the other, "
                                + "from Ophiuchus, may coincide with Kepler's 1604 supernova. All the x-ray sources are fairly close to the galactic plane."),
                new DoubleField(0.667832788) };

        IField[] fields3 = { new IntegerField(4566015), new TextField(""),
                new TextField("Significance of milk pH in newborn infants."), new TextField("V C Harrison, G Peat"),
                new StringField("4-5839 Dec 2, 1972"), new TextField("British medical journal"), new TextField(""),
                new TextField("Infant Nutritional Physiological Phenomena, Infant, Newborn, Milk"),
                new TextField(
                        "Bottle-fed infants do not gain weight as rapidly as breast-fed babies during the first week of life. This "
                                + "weight lag can be corrected by the addition of a small amount of alkali (sodium bicarbonate or trometamol) to "
                                + "the feeds. The alkali corrects the acidity of cow's milk which now assumes some of the properties of human breast "
                                + "milk. It has a bacteriostatic effect on specific Escherichia coli in vitro, and in infants it produces a stool with"
                                + " a preponderance of lactobacilli over E. coli organisms. When alkali is removed from the milk there is a decrease in"
                                + " the weight of an infant and the stools contain excessive numbers of E. coli bacteria.A pH-corrected milk appears to"
                                + " be more physiological than unaltered cow's milk and may provide some protection against gastroenteritis in early "
                                + "life. Its bacteriostatic effect on specific E. coli may be of practical significance in feed preparations where "
                                + "terminal sterilization and refrigeration are not available. The study was conducted during the week after birth, and "
                                + "no conclusions are derived for older infants. The long-term effects of trometamol are unknown. No recommendation can "
                                + "be given for the addition of sodium bicarbonate to milks containing a higher content of sodium."),
                new DoubleField(0.667832788) };

        Tuple tuple1 = new Tuple(SCHEMA_MEDLINE, fields1);
        Tuple tuple2 = new Tuple(SCHEMA_MEDLINE, fields2);
        Tuple tuple3 = new Tuple(SCHEMA_MEDLINE, fields3);

        return Arrays.asList(tuple1, tuple2, tuple3);
    }

}
