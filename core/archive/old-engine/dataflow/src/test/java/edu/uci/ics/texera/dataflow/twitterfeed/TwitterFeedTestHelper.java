package edu.uci.ics.texera.dataflow.twitterfeed;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.twitter.hbc.core.endpoint.Location;


import static edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils.TwitterSchema.TWEET_COORDINATES;
import static edu.uci.ics.texera.dataflow.twitterfeed.TwitterUtils.TwitterSchema.TWEET_LOCATION;

/**
 * Created by Chang on 7/13/17.
 */
public class TwitterFeedTestHelper {

    public static List<Tuple> getQueryResults(List<String> queryList, String locationList, List<String> languageList, int limit) throws TexeraException {
        TwitterFeedSourcePredicate predicate = new TwitterFeedSourcePredicate(10, queryList, locationList, languageList, null, null, null, null);
        TwitterFeedOperator twitterFeedOperator = new TwitterFeedOperator(predicate);
        twitterFeedOperator.setLimit(limit);
        twitterFeedOperator.setTimeout(20);
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();

        twitterFeedOperator.open();
        while ((tuple = twitterFeedOperator.getNextTuple()) != null) {
            results.add(tuple);
        }
        twitterFeedOperator.close();
        return results;
    }

    public static boolean containsQuery(List<Tuple> resultList, List<String> queryList, List<String> attributeList) {
        if (resultList.isEmpty()) return false;
        for (Tuple tuple : resultList) {
            List<String> toMatch = new ArrayList<>();
            for (String attribute : attributeList) {
                toMatch.addAll(queryList.stream()
                        .filter(s -> tuple.getField(attribute).getValue().toString().toLowerCase().contains(s.toLowerCase()))
                        .collect(Collectors.toList()));
            }
            if (toMatch.isEmpty()) return false;
        }
        return true;
    }

    public static boolean checkKeywordInAttributes(List<Tuple> exactResult, List<String> queryList, List<String> attributeList) {
        List<String> toMatch = new ArrayList<>();
        for (Tuple tuple : exactResult) {
            for (String attribute : attributeList) {
                toMatch.addAll(queryList.stream()
                        .filter(s -> tuple.getField(attribute).getValue().toString().toLowerCase().contains(s.toLowerCase()))
                        .collect(Collectors.toList()));
            }

        }
        if (toMatch.isEmpty()) return false;
        return true;
    }

    public static boolean inLocation(List<Tuple> tupleList, String location) {
        List<String> boundingCoordinate = Arrays.asList(location.trim().split(","));
        List<Double> boundingBox = new ArrayList<>();
        boundingCoordinate.stream().forEach(s -> boundingBox.add(Double.parseDouble(s.trim())));
        Location geoBox = new Location(new Location.Coordinate(boundingBox.get(1), boundingBox.get(0)), new Location.Coordinate(boundingBox.get(3), boundingBox.get(2)));
        Location.Coordinate southwestCoordinate = geoBox.southwestCoordinate();
        Location.Coordinate northeastCoordinate = geoBox.northeastCoordinate();

        for (Tuple tuple : tupleList) {
            if (!tuple.getField(TWEET_COORDINATES).getValue().toString().equals("n/a") && !tuple.getField(TWEET_LOCATION).getValue().toString().equals("n/a")) {
                List<String> coordString = Arrays.asList(tuple.getField(TWEET_COORDINATES).getValue().toString().split(","));
                Location.Coordinate coordinate = new Location.Coordinate(Double.parseDouble(coordString.get(0)), Double.parseDouble(coordString.get(1)));
                if (!(coordinate.latitude() >= southwestCoordinate.latitude() &&
                        coordinate.longitude() >= southwestCoordinate.longitude() &&
                        coordinate.latitude() <= northeastCoordinate.latitude() &&
                        coordinate.longitude() <= northeastCoordinate.longitude())) {
                    if (!tuple.getField(TWEET_LOCATION).getValue().toString().contains("United States")) {
                        return false;
                    }

                }

            }

        }

        return true;
    }

    public static boolean compareTuple(List<Tuple> outputTuple, Tuple expectedTuple) {
        for(Tuple t : outputTuple) {
            for (String attribute : expectedTuple.getSchema().getAttributeNames()) {
                if (!attribute.equals("_id")) {
                    if (! t.getField(attribute).getValue().toString().equals(expectedTuple.getField(attribute).getValue().toString())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}
