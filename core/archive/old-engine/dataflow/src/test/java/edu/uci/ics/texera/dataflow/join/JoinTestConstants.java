package edu.uci.ics.texera.dataflow.join;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

public class JoinTestConstants {
    
    public static final String ID = "id";
    public static final String AUTHOR = "author";
    public static final String TITLE = "title";
    public static final String PAGES = "numberOfPages";
    public static final String REVIEW = "reviewOfBook";
    
    public static final Attribute ID_ATTR = new Attribute(ID, AttributeType.INTEGER);
    public static final Attribute AUTHOR_ATTR = new Attribute(AUTHOR, AttributeType.STRING);
    public static final Attribute TITLE_ATTR = new Attribute(TITLE, AttributeType.STRING);
    public static final Attribute PAGES_ATTR = new Attribute(PAGES, AttributeType.INTEGER);
    public static final Attribute REVIEW_ATTR = new Attribute(REVIEW, AttributeType.TEXT);
    
    public static final Schema BOOK_SCHEMA = new Schema(ID_ATTR, AUTHOR_ATTR, TITLE_ATTR, PAGES_ATTR, REVIEW_ATTR);
       
    public static final List<Tuple> bookGroup1 = Arrays.asList(     
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(52), new StringField("Mary Roach"),
                    new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                    new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                            + "gastrointestinal tract interesting (sometimes "
                            + "hilariously so), and pop science writer Mary Roach is " + "always up to the task.")),
            new Tuple(BOOK_SCHEMA,               
                    new IntegerField(51), new StringField("author unknown"),
                    new StringField("typical"), new IntegerField(300),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(53), new StringField("Noah Hawley"),
                    new StringField("Before the Fall"), new IntegerField(400),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(54), new StringField("Andria Williams"),
                    new StringField("The Longest Night: A Novel"), new IntegerField(400),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(55), new StringField("Matti Friedman"),
                    new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")));
    
    public static final List<Tuple> bookGroup2 = Arrays.asList(
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(61), new StringField("book author"),
                    new StringField("actually typical"), new IntegerField(700),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(62), new StringField("Siddhartha Mukherjee"),
                    new StringField("The Gene: An Intimate History"), new IntegerField(608),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(63), new StringField("Paul Kalanithi"),
                    new StringField("When Breath Becomes Air"), new IntegerField(256),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(64), new StringField("Matthew Desmond"),
                    new StringField("Evicted: Poverty and Profit in the " + "American City"),
                    new IntegerField(432),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")),
            new Tuple(BOOK_SCHEMA,
                    new IntegerField(65), new StringField("Sharon Guskin"),
                    new StringField("The Forgetting Time: A Novel"), new IntegerField(368),
                    new TextField("Review of a Book. This is a typical "
                            + "review. This is a test. A book review " + "test. A test to test queries without "
                            + "actually using actual review. From " + "here onwards, we can pretend this to "
                            + "be actually a review even if it is not " + "your typical book review.")));

    
    
    public static final String NEWS_NUMBER = "news_number";
    public static final String NEWS_TITLE = "news_title";
    public static final String NEWS_BODY = "news_body";
    
    public static final Attribute NEWS_NUM_ATTR = new Attribute(NEWS_NUMBER, AttributeType.INTEGER);
    public static final Attribute NEWS_TITLE_ATTR = new Attribute(NEWS_TITLE, AttributeType.TEXT);
    public static final Attribute NEWS_BODY_ATTR = new Attribute(NEWS_BODY, AttributeType.TEXT);
    
    public static final Schema NEWS_SCHEMA = new Schema(NEWS_NUM_ATTR, NEWS_TITLE_ATTR, NEWS_BODY_ATTR);
    
    public static List<Tuple> getNewsTuples() {
        return Arrays.asList(
                new Tuple(NEWS_SCHEMA, 
                        new IntegerField(1),
                        new TextField("UCI marchers protest as Trump begins his presidency"),
                        new TextField("a few hours after Donald Trump was sworn in Friday as the nation’s 45th president, "
                                + "a line of more than 100 UC Irvine faculty members and students took to the campus "
                                + "in pouring rain to demonstrate their opposition to his policies on immigration and "
                                + "other issues and urge other opponents to keep organizing during Trump’s presidency.")
                        ),

                new Tuple(NEWS_SCHEMA, 
                        new IntegerField(2),
                        new TextField("Alternative Facts and the Costs of Trump-Branded Reality"),
                        new TextField("When Donald J. Trump swore the presidential oath on Friday, he assumed "
                                + "responsibility not only for the levers of government but also for one of "
                                + "the United States’ most valuable assets, battered though it may be: its credibility. "
                                + "The country’s sentimental reverence for truth and its jealously guarded press freedoms, "
                                + "while never perfect, have been as important to its global standing as the strength of "
                                + "its military and the reliability of its currency. It’s the bedrock of that "
                                + "American exceptionalism we’ve heard so much about for so long.")
                        ),
                new Tuple(NEWS_SCHEMA, 
                        new IntegerField(3),
                        new TextField("Samsung Explains Note 7 Battery Explosions, And Turns Crisis Into Opportunity"),
                        new TextField("Samsung launched the Galaxy Note 7 to record preorders and sales in August, "
                                + "but the rosy start soon turned sour. Samsung had to initiate a recall in September of "
                                + "the first version of the Note 7 due to faulty batteries that overheated and exploded. "
                                + "By October it had to recall over 2 million devices and discontinue the product. "
                                + "It’s estimated that the recall will cost Samsung $5.3 billion.")
                        ),
                new Tuple(NEWS_SCHEMA,
                        new IntegerField(4),
                        new TextField("This is how Samsung plans to prevent future phones from catching fire"),
                        new TextField("Samsung said that it has implemented a new eight-step testing process for "
                                + "its lithium ion batteries, and that it’s forming a battery advisory board as well, "
                                + "comprised of academics from Cambridge, Berkeley, and Stanford. "
                                + "Note, this is for all lithium ion batteries in Samsung products, "
                                + "not just Note phablets or the anticipated Galaxy S8 phone.")
                        )
                );
    }
    
    
}
