package edu.uci.ics.textdb.sandbox.team6lucenetweetexample;

public class Data {
    

        private static final Tweet[] TWEETS = {
                new Tweet(
                        "1467810672",
                        "Mon Apr 06 22:19:49 PDT 2009",
                        "scotthamilton",
                        "is upset that he can't update his Facebook by texting it... and might cry as a result  School today also. Blah!"),
                new Tweet(
                        "1467810917",
                        "Mon Apr 06 22:19:53 PDT 2009",
                        "mattycus",
                        "@Kenichan I dived many times for the ball. Managed to save 50%  The rest go out of bounds"),
                new Tweet(
                        "1467811184",
                        "Mon Apr 06 22:19:57 PDT 2009",
                        "ElleCTF",
                        "my whole body feels itchy and like its on fire "),
                new Tweet(
                        "1467811193",
                        "Mon Apr 06 22:19:57 PDT 2009",
                        "Karoli",
                        "@nationwideclass no, it's not behaving at all. i'm mad. why am i here? because I can't see you all over there. "),
                new Tweet(
                        "1467811372",
                        "Mon Apr 06 22:20:00 PDT 2009",
                        "joy_wolf",
                        "@Kwesidei not the whole crew "),
                new Tweet(
                        "1467811592",
                        "Mon Apr 06 22:20:03 PDT 2009",
                        "mybirch",
                        "Need a hug "),
                new Tweet(
                        "1467811594",
                        "Mon Apr 06 22:20:03 PDT 2009",
                        "coZZ",
                        "@LOLTrish hey  long time no see! Yes.. Rains a bit ,only a bit  LOL , I'm fine thanks , how's you ?"),
                new Tweet(
                        "1467811795",
                        "Mon Apr 06 22:20:05 PDT 2009",
                        "2Hood4Hollywood",
                        "@Tatiana_K nope they didn't have it "),
                new Tweet(
                        "1467812025",
                        "Mon Apr 06 22:20:09 PDT 2009",
                        "mimismo",
                        "@twittera que me muera ? "),
                new Tweet(
                        "1467812416",
                        "Mon Apr 06 22:20:16 PDT 2009",
                        "erinx3leannexo",
                        "spring break in plain city... it's snowing "),
        };

    public static Tweet[] getTweets() {
    	return TWEETS;
    }

    public static Tweet getTweet(String id) {
        for (Tweet tweet : TWEETS) {
            if (id.equals(tweet.getId())) {
                return tweet;
            }
        }
        return null;
    }
}
