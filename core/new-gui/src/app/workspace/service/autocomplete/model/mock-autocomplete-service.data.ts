import { SourceTableNamesAPIResponse, SuccessExecutionResult } from '../../../types/autocomplete.interface';
import { OperatorSchema } from '../../../types/operator-schema.interface';

/**
 * Export constants related to the source table names present at the server
 */
// TODO: All responses from backend should come as a JSON object. The JSON message below will
// have to be changed to be a JSON object. Same change will need to be done on the backend.
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
    '2': [
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

export const mockAutocompletedOperatorSchema: ReadonlyArray<OperatorSchema> =
[
    {
      operatorType: 'ScanSource',
      additionalMetadata: {
        advancedOptions: [],
        operatorDescription: 'Read records from a table one by one',
        operatorGroupName: 'Source',
        numInputPorts: 0,
        numOutputPorts: 1,
        userFriendlyName: 'Source: Scan'
      },
      jsonSchema: {
        id: 'urn:jsonschema:edu:uci:ics:texera:dataflow:source:scan:ScanSourcePredicate',
        properties: {
          tableName: {
            type: 'string',
            enum: ['promed', 'twitter_sample']
          }
        },
        required: [
          'tableName'
        ],
        type: 'object'
      }
    },
    {
      operatorType: 'NlpSentiment',
      additionalMetadata: {
        advancedOptions: [],
        operatorDescription: 'Sentiment analysis based on Stanford NLP package',
        operatorGroupName: 'Analysis',
        numInputPorts: 1,
        numOutputPorts: 1,
        userFriendlyName: 'Sentiment Analysis'
      },
      jsonSchema: {
        id: 'urn:jsonschema:edu:uci:ics:texera:dataflow:nlp:sentiment:NlpSentimentPredicate',
        properties: {
          attribute: {
            type: 'string',
            enum: ['city',
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
            'user_friends_count']
          },
          resultAttribute: {
            type: 'string'
          }
        },
        required: [
          'attribute',
          'resultAttribute'
        ],
        type: 'object'
      }
    },
    {
      operatorType: 'ViewResults',
      additionalMetadata: {
        advancedOptions: [],
        operatorDescription: 'View the results of the workflow',
        operatorGroupName: 'View Results',
        numInputPorts: 1,
        numOutputPorts: 0,
        userFriendlyName: 'View Results'
      },
      jsonSchema: {
        id: 'urn:jsonschema:edu:uci:ics:texera:dataflow:sink:tuple:TupleSinkPredicate',
        properties: {
          limit: {
            default: 10,
            type: 'integer'
          },
          'offset': {
            default: 0,
            type: 'integer'
          }
        },
        type: 'object'
      }
    }
  ];
