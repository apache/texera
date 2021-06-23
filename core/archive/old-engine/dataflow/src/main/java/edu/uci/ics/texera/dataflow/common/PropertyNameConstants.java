package edu.uci.ics.texera.dataflow.common;

/**
 * PropertyNameConstants defines the key names 
 *   in the JSON representation of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class PropertyNameConstants {

    private PropertyNameConstants() {};
    
    // operator metadata names, used in generating operator json schema
    public static final String USER_FRIENDLY_NAME = "userFriendlyName";
    public static final String OPERATOR_DESCRIPTION = "operatorDescription";
    public static final String OPERATOR_GROUP_NAME = "operatorGroupName";
    public static final String OPERATOR_COLOR = "operatorColor";
    public static final String OPERATOR_IMAGE_PATH = "operatorImagePath";
    public static final String HIDDEN_PROPERTIES = "hiddenProperties";
    public static final String PROPERTIES_DESCRIPTION = "propertyDescription";
    
    // logical plan property names
    public static final String OPERATOR_ID = "operatorID";
    public static final String OPERATOR_TYPE = "operatorType";
    public static final String ORIGIN_OPERATOR_ID = "origin";
    public static final String DESTINATION_OPERATOR_ID = "destination";
    public static final String OPERATOR_LIST = "operators";
    public static final String OPERATOR_LINK_LIST = "links";
    
    // common operator property names
    public static final String ATTRIBUTE_NAMES = "attributes";
    public static final String ATTRIBUTE_NAME = "attribute";
    public static final String RESULT_ATTRIBUTE_NAME = "resultAttribute";
    public static final String LUCENE_ANALYZER_STRING = "luceneAnalyzer";
    public static final String SPAN_LIST_NAME = "spanListName";
    public static final String TABLE_NAME = "tableName";
    public static final String FILE_NAME = "fileName";
    public static final String FILE_PATH = "filePath";
    public static final String FILE_FORMAT = "fileFormat";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    public static final String ADD_SPANS = "addSpans";
    
    // related to aggregator operator
    public static final String AGGREGATOR_TYPE = "aggregator";
    public static final String ATTRIBUTE_AGGREGATOR_RESULT_LIST = "listOfAggregations";
    
    // related to keyword matcher
    public static final String KEYWORD_QUERY = "query";
    public static final String KEYWORD_MATCHING_TYPE = "matchingType";
    
    // related to dictionary matcher
    public static final String DICTIONARY = "dictionary";
    public static final String DICTIONARY_ENTRIES = "dictionaryEntries";
    
    // related to regex matcher
    public static final String REGEX = "regex";
    public static final String REGEX_IGNORE_CASE = "regexIgnoreCase";
    public static final String REGEX_USE_INDEX = "regexUseIndex";
    
    // related to fuzzy token matcher
    public static final String FUZZY_TOKEN_QUERY = "query";
    public static final String FUZZY_TOKEN_THRESHOLD_RATIO = "thresholdRatio";
    
    // related to nlp extractor
    public static final String NLP_ENTITY_TYPE = "nlpEntityType";
    
    // related to nlp splitter
    public static final String NLP_LANGUAGE = "nlpLanguage";
    public static final String NLP_OUTPUT_TYPE = "splitOption";
    public static final String NLP_SPLIT_KEY = "nlpSplit";

    // related to regex splitter
    public static final String SPLIT_TYPE = "splitType";
    public static final String SPLIT_REGEX = "splitRegex";
    public static final String REGEX_OUTPUT_TYPE = "splitOption";
    public static final String REGEX_SPLIT_KEY = "regexSplit";

    // related to sampler
    public static final String SAMPLE_SIZE = "sampleSize";
    public static final String SAMPLE_TYPE = "sampleType";
    
    // related to file source
    public static final String FILE_MAX_DEPTH = "maxDepth";
    public static final String FILE_RECURSIVE = "recursive";
    public static final String FILE_ALLOWED_EXTENSIONS = "allowedExtensions";
    
    // related to join
    public static final String INNER_ATTRIBUTE_NAME = "innerAttribute";
    public static final String OUTER_ATTRIBUTE_NAME = "outerAttribute";
    public static final String SPAN_DISTANCE = "spanDistance";
    public static final String JOIN_SIMILARITY_THRESHOLD = "similarityThreshold";
    
    // related to asterix connector
    public static final String ASTERIX_HOST = "host";
    public static final String ASTERIX_PORT = "port";
    public static final String ASTERIX_DATAVERSE = "dataverse";
    public static final String ASTERIX_DATASET = "dataset";
    public static final String ASTERIX_QUERY_FIELD = "queryField";
    public static final String ASTERIX_START_DATE = "startDate";
    public static final String ASTERIX_END_DATE = "endDate";

    // related to ComparableMatcher
    public static final String COMPARISON_TYPE = "comparisonType";
    public static final String COMPARE_TO_VALUE = "compareTo";
    
    // related to MysqlSink
    public static final String MYSQL_HOST = "host";
    public static final String MYSQL_PORT = "port";
    public static final String MYSQL_DATABASE = "database";
    public static final String MYSQL_TABLE = "table";
    public static final String MYSQL_USERNAME = "username";
    public static final String MYSQL_PASSWORD = "password";
    public static final String MYSQL_LIMIT = "limit";
    public static final String MYSQL_OFFSET = "offset";
    
    //related to MysqlSource
    public static final String MYSQL_KEYWORDS = "boolean expression";
    public static final String MYSQL_COLUMN = "column";
    
    // related to Twitter converter
    public static final String TWITTER_CONVERTER_RAW_JSON = "rawJsonStringAttributeName";

    // related to TwitterFeed Operator
    public static final String TWEET_NUM = "tweetNum";
    public static final String TWEET_QUERY_LIST = "keywordList";
    public static final String TWEET_LOCATION_LIST = "locationList";
    public static final String TWEET_LANGUAGE_LIST = "languageList";
    public static final String TWEET_CUSTOMER_KEY = "customerKey";
    public static final String TWEET_CUSTOMER_SECRET = "customerSecret";
    public static final String TWEET_TOKEN = "token";
    public static final String TWEET_TOKEN_SECRET = "tokenSecret";

    public static final String EMPTY_NAME_EXCEPTION = "Table Name Cannot be Empty";
    public static final String EMPTY_REGEX_EXCEPTION = "regex should not be empty";
    public static final String EMPTY_QUERY_EXCEPTION = "query should not be empty";
    public static final String NAME_NOT_MATCH_EXCEPTION = "inner attribute name and outer attribute name are different";
    public static final String INVALID_THRESHOLD_EXCEPTION = "threshold ratio should be between 0.0 and 1.0";
    public static final String INVALID_SAMPLE_SIZE_EXCEPTION = "Sample size should be greater than 0.";
    public static final String INVALID_LIMIT_EXCEPTION = "limit must be greater than or equal to 0";
    public static final String INVALID_OFFSET_EXCEPTION = "offset must be greater than or equal to 0";


    // related to Visualization Operator
    public static final String NAME_COLUMN = "nameColumn";
    public static final String DATA_COLUMN = "dataColumn";
    public static final String PRUNE_RATIO = "pruneRatio";

    public static final String CHART_STYLE = "chartStyle";

    public static final String WORD_COLUMN = "wordColumn";
    public static final String COUNT_COLUMN = "countColumn";


    // related to duplicated NltkSentiment
    public static final String NLTK_BATCH_SIZE = "batchSize";
    public static final String NLTK_MODEL = "inputAttributeModel";

    public static final String ARROW_CHUNK_SIZE = "arrowBatchSize";

}
