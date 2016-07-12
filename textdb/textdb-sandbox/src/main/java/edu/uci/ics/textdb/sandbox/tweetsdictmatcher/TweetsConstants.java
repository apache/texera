package edu.uci.ics.textdb.sandbox.tweetsdictmatcher;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

public class TweetsConstants {
	public static final String ID_STR = "id_str";
	public static final String TEXT = "text";
	public static final String HASHTAGS = "hashtags";
	
	public static final String ENTITIES = "entities";
	
	public static final Attribute ID_STR_ATTR = new Attribute(ID_STR, FieldType.TEXT);
	public static final Attribute TEXT_ATTR = new Attribute(TEXT, FieldType.TEXT);
	public static final Attribute HASHTAGS_ATTR = new Attribute(HASHTAGS, FieldType.TEXT);
	 

	public static final Attribute[] ATTRIBUTES_TWEETS = {ID_STR_ATTR, TEXT_ATTR, HASHTAGS_ATTR	};
	
	public static final Schema SCHEMA_TWEETS = new Schema(ATTRIBUTES_TWEETS);
}
