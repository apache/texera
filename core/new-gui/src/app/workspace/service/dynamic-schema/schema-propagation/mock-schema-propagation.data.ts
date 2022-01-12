import { SchemaPropagationResponse } from "./schema-propagation.service";

/**
 * Export constants related to the source table names present at the server
 */

export const mockSchemaPropagationOperatorID = "2";

export const mockSchemaPropagationResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSchemaPropagationOperatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "user_screen_name", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
        { attributeName: "tweet_link", attributeType: "string" },
        { attributeName: "payload", attributeType: "string" },
        { attributeName: "user_followers_count", attributeType: "integer" },
        { attributeName: "user_link", attributeType: "string" },
        { attributeName: "_id", attributeType: "string" },
        { attributeName: "text", attributeType: "string" },
        { attributeName: "state", attributeType: "string" },
        { attributeName: "create_at", attributeType: "string" },
        { attributeName: "user_description", attributeType: "string" },
        { attributeName: "user_friends_count", attributeType: "integer" },
      ],
    ],
  },
};

export const mockEmptySchemaPropagationResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {},
};

export const mockAutocompleteAPIEmptyResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {},
};
