import { SourceTableNamesAPIResponse } from '../../../types/source-table-names.interface';

/**
 * Export constants related to the source table names present at the server
 */
export const mockSourceTableAPIResponse: Readonly<SourceTableNamesAPIResponse> = {
  code: 0,
  message: `[
    {
      \"tableName\":\"promed\",
      \"schema\":{
        \"attributes\":[
          {\"attributeName\":\"_id\",
          \"attributeType\":\"_id\"},
          {\"attributeName\":\"id\",
          \"attributeType\":\"string\"},
          {\"attributeName\":\"content\",
          \"attributeType\":\"text\"}
        ]
      }
    },
    {\"tableName\":\"twitter_sample\",
    \"schema\":{
      \"attributes\":[
        {\"attributeName\":\"_id\",
        \"attributeType\":\"_id\"},
        {\"attributeName\":\"text\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"tweet_link\",
        \"attributeType\":\"string\"},
        {\"attributeName\":\"user_link\",
        \"attributeType\":\"string\"},
        {\"attributeName\":\"user_screen_name\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"user_name\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"user_description\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"user_followers_count\",
        \"attributeType\":\"integer\"},
        {\"attributeName\":\"user_friends_count\",
        \"attributeType\":\"integer\"},
        {\"attributeName\":\"state\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"county\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"city\",
        \"attributeType\":\"text\"},
        {\"attributeName\":\"create_at\",
        \"attributeType\":\"string\"}
      ]
    }
  }
]`
};
