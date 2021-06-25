package edu.uci.ics.texera.dataflow.join;

import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.join.SimilarityJoinPredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Tests the SimilarityJoinPredicate, which joins two tuples
 *   based on the spans' similarity.
 * 
 * @author Zuozhi Wang
 *
 */
public class SimilarityJoinTest {

    public static final String NEWS_TABLE_OUTER = JoinTestHelper.NEWS_TABLE_OUTER;
    public static final String NEWS_TABLE_INNER = JoinTestHelper.NEWS_TABLE_INNER;
    
    public static final KeywordMatchingType conjunction = KeywordMatchingType.CONJUNCTION_INDEXBASED;
    public static final KeywordMatchingType phrase = KeywordMatchingType.PHRASE_INDEXBASED;

    
    /*
     * The annotators @BeforeClass and @AfterClass are used instead of @Before and @After.
     * 
     * The difference is that:
     *   @Before and @After are executed before and after EACH test case.
     *   @BeforeClass and @AfterClass are executed once before ALL the test begin and ALL the test have finished.
     * 
     * We don't want to create and delete the tables on every test case, 
     *   therefore BeforeClass and AfterClass are better options.
     *   
     */
    @BeforeClass
    public static void setup() throws TexeraException {
        // writes the test tables before ALL tests
        JoinTestHelper.createTestTables();
    }
    
    @AfterClass
    public static void cleanUp() throws TexeraException {
        // deletes the test tables after ALL tests
        JoinTestHelper.deleteTestTables();
    }

    @After
    public void clear() throws TexeraException {
        JoinTestHelper.clearTestTables();
    }


    /*
     * Tests the Similarity Join Predicate on two similar words:
     *   Donald J. Trump
     *   Donald Trump
     * Under the condition of similarity (NormalizedLevenshtein) > 0.8, these two words should match.
     *
     */
    @Test
    public void test1() throws TexeraException {
        JoinTestHelper.insertToTable(NEWS_TABLE_OUTER, JoinTestConstants.getNewsTuples().get(0));
        JoinTestHelper.insertToTable(NEWS_TABLE_INNER, JoinTestConstants.getNewsTuples().get(1));

        String trumpRegex = "[Dd]onald.{1,5}[Tt]rump";

        RegexMatcher regexMatcherInner = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_INNER,
                trumpRegex, JoinTestConstants.NEWS_BODY);
        RegexMatcher regexMatcherOuter = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_OUTER,
                trumpRegex, JoinTestConstants.NEWS_BODY);

        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate(JoinTestConstants.NEWS_BODY, 0.8);
        List<Tuple> results = JoinTestHelper.getJoinDistanceResults(
                regexMatcherInner, regexMatcherOuter, similarityJoinPredicate, Integer.MAX_VALUE, 0);

        
        Schema joinInputSchema = new Schema.Builder().add(JoinTestConstants.NEWS_SCHEMA).add(SchemaConstants.SPAN_LIST_ATTRIBUTE).build();
        Schema resultSchema = similarityJoinPredicate.generateOutputSchema(joinInputSchema, joinInputSchema);

        List<Span> resultSpanList = Arrays.asList(
                new Span("inner_"+JoinTestConstants.NEWS_BODY, 5, 20, trumpRegex, "Donald J. Trump", -1),
                new Span("outer_"+JoinTestConstants.NEWS_BODY, 18, 30, trumpRegex, "Donald Trump", -1)
        );

        Tuple resultTuple = new Tuple(resultSchema,
                new IDField(UUID.randomUUID().toString()),
                new IntegerField(2),
                new TextField("Alternative Facts and the Costs of Trump-Branded Reality"),
                new TextField("When Donald J. Trump swore the presidential oath on Friday, he assumed "
                        + "responsibility not only for the levers of government but also for one of "
                        + "the United States’ most valuable assets, battered though it may be: its credibility. "
                        + "The country’s sentimental reverence for truth and its jealously guarded press freedoms, "
                        + "while never perfect, have been as important to its global standing as the strength of "
                        + "its military and the reliability of its currency. It’s the bedrock of that "
                        + "American exceptionalism we’ve heard so much about for so long."),
                new IntegerField(1),
                new TextField("UCI marchers protest as Trump begins his presidency"),
                new TextField("a few hours after Donald Trump was sworn in Friday as the nation’s 45th president, "
                        + "a line of more than 100 UC Irvine faculty members and students took to the campus "
                        + "in pouring rain to demonstrate their opposition to his policies on immigration and "
                        + "other issues and urge other opponents to keep organizing during Trump’s presidency."),
                new ListField<>(resultSpanList));

        Assert.assertTrue(TestUtils.equals(Arrays.asList(resultTuple), results));
    }

    /*
     * Tests the Similarity Join Predicate on two similar words:
     *   Donald J. Trump
     *   Donald Trump
     * Under the condition of similarity (NormalizedLevenshtein) > 0.9, these two words should NOT match.
     *
     */
    @Test
    public void test2() throws TexeraException {
        JoinTestHelper.insertToTable(NEWS_TABLE_OUTER, JoinTestConstants.getNewsTuples().get(0));
        JoinTestHelper.insertToTable(NEWS_TABLE_INNER, JoinTestConstants.getNewsTuples().get(1));

        String trumpRegex = "[Dd]onald.{1,5}[Tt]rump";

        RegexMatcher regexMatcherInner = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_INNER,
                trumpRegex, JoinTestConstants.NEWS_BODY);
        RegexMatcher regexMatcherOuter = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_OUTER,
                trumpRegex, JoinTestConstants.NEWS_BODY);

        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate(JoinTestConstants.NEWS_BODY, 0.9);
        List<Tuple> results = JoinTestHelper.getJoinDistanceResults(
                regexMatcherInner, regexMatcherOuter, similarityJoinPredicate, Integer.MAX_VALUE, 0);

        Assert.assertTrue(results.isEmpty());
    }

    /*
     * Tests the Similarity Join Predicate on two similar words:
     *   Galaxy S8
     *   Galaxy Note 7
     * Under the condition of similarity (NormalizedLevenshtein) > 0.5, these two words should match.
     *
     */
    @Test
    public void test3() throws TexeraException {
        JoinTestHelper.insertToTable(NEWS_TABLE_OUTER, JoinTestConstants.getNewsTuples().get(2));
        JoinTestHelper.insertToTable(NEWS_TABLE_INNER, JoinTestConstants.getNewsTuples().get(3));

        String phoneRegex = "[Gg]alaxy.{1,6}\\d";

        RegexMatcher regexMatcherInner = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_INNER,
                phoneRegex, JoinTestConstants.NEWS_BODY);
        RegexMatcher regexMatcherOuter = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_OUTER,
                phoneRegex, JoinTestConstants.NEWS_BODY);

        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate(JoinTestConstants.NEWS_BODY, 0.5);
        List<Tuple> results = JoinTestHelper.getJoinDistanceResults(
                regexMatcherInner, regexMatcherOuter, similarityJoinPredicate, Integer.MAX_VALUE, 0);

        Schema joinInputSchema = new Schema.Builder().add(JoinTestConstants.NEWS_SCHEMA).add(SchemaConstants.SPAN_LIST_ATTRIBUTE).build();
        Schema resultSchema = similarityJoinPredicate.generateOutputSchema(joinInputSchema, joinInputSchema);

        List<Span> resultSpanList = Arrays.asList(
                new Span("inner_"+JoinTestConstants.NEWS_BODY, 327, 336, phoneRegex, "Galaxy S8", -1),
                new Span("outer_"+JoinTestConstants.NEWS_BODY, 21, 34, phoneRegex, "Galaxy Note 7", -1)
        );

        Tuple resultTuple = new Tuple(resultSchema,
                new IDField(UUID.randomUUID().toString()),
                new IntegerField(4),
                new TextField("This is how Samsung plans to prevent future phones from catching fire"),
                new TextField("Samsung said that it has implemented a new eight-step testing process for "
                        + "its lithium ion batteries, and that it’s forming a battery advisory board as well, "
                        + "comprised of academics from Cambridge, Berkeley, and Stanford. "
                        + "Note, this is for all lithium ion batteries in Samsung products, "
                        + "not just Note phablets or the anticipated Galaxy S8 phone."),
                new IntegerField(3),
                new TextField("Samsung Explains Note 7 Battery Explosions, And Turns Crisis Into Opportunity"),
                new TextField("Samsung launched the Galaxy Note 7 to record preorders and sales in August, "
                        + "but the rosy start soon turned sour. Samsung had to initiate a recall in September of "
                        + "the first version of the Note 7 due to faulty batteries that overheated and exploded. "
                        + "By October it had to recall over 2 million devices and discontinue the product. "
                        + "It’s estimated that the recall will cost Samsung $5.3 billion."),
                new ListField<>(resultSpanList));

        Assert.assertTrue(TestUtils.equals(Arrays.asList(resultTuple), results));
    }

    /*
     * Tests the Similarity Join Predicate on two similar words:
     *   Galaxy S8
     *   Galaxy Note 7
     * Under the condition of similarity (NormalizedLevenshtein) > 0.8, these two words should NOT match.
     *
     */
    @Test
    public void test4() throws TexeraException {
        JoinTestHelper.insertToTable(NEWS_TABLE_OUTER, JoinTestConstants.getNewsTuples().get(2));
        JoinTestHelper.insertToTable(NEWS_TABLE_INNER, JoinTestConstants.getNewsTuples().get(3));

        String phoneRegex = "[Gg]alaxy.{1,6}\\d";

        RegexMatcher regexMatcherInner = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_INNER,
                phoneRegex, JoinTestConstants.NEWS_BODY);
        RegexMatcher regexMatcherOuter = JoinTestHelper.getRegexMatcher(JoinTestHelper.NEWS_TABLE_OUTER,
                phoneRegex, JoinTestConstants.NEWS_BODY);

        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate(JoinTestConstants.NEWS_BODY, 0.8);
        List<Tuple> results = JoinTestHelper.getJoinDistanceResults(
                regexMatcherInner, regexMatcherOuter, similarityJoinPredicate, Integer.MAX_VALUE, 0);

        Assert.assertTrue(results.isEmpty());
    }


}
