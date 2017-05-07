package edu.uci.ics.textdb.perftest.twitter;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

public class TwitterSchema {
    
    public static String ID = "id";
    public static Attribute ID_ATTRIBUTE = new Attribute(ID, AttributeType.STRING);
    
    public static String CREATE_AT = "create_at";
    public static Attribute CREATE_AT_ATTRIBUTE = new Attribute(CREATE_AT, AttributeType.STRING);
    
    public static String USER_NAME = "user_name";
    public static Attribute USER_NAME_ATTRIBUTE = new Attribute(USER_NAME, AttributeType.STRING);
    
    public static String TEXT = "text";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);
    
    public static Schema TWITTER_SCHEMA = new Schema(
            ID_ATTRIBUTE, CREATE_AT_ATTRIBUTE, USER_NAME_ATTRIBUTE, TEXT_ATTRIBUTE);

}
