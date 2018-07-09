import { SourceTableNamesAPIResponse, SuccessExecutionResult } from '../types/autocomplete.interface';

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

export const mockAutocompleteAPISchemaSuggestionResponse: Readonly<SuccessExecutionResult> = {
  code: 0,
  result: {
    'operator-6383932c-f846-4ac8-bc9d-52d9ddff86f7': [
      'city',
      'user_screen_name',
      'user_name',
      'county',
      'tweet_link',
      'payload',
      'user_followers_count',
      'user_link',
      '_id',
      'text',
      'state',
      'create_at',
      'user_description',
      'user_friends_count'
    ]
  }
};

export const mockAutocompleteAPIEmptyResponse: Readonly<SuccessExecutionResult> = {
  code: 0,
  result: { }
};
