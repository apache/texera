import { SchemaPropagationResponse } from './schema-propagation.service';

/**
 * Export constants related to the source table names present at the server
 */

export const mockSchemaPropagationOperatorID = '2';

export const mockSchemaPropagationResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSchemaPropagationOperatorID]: [
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

export const mockEmptySchemaPropagationResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
  }
};

export const mockAutocompleteAPIEmptyResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: { }
};


