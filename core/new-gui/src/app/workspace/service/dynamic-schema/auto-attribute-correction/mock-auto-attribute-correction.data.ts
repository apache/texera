import { OperatorLink, OperatorPredicate } from "src/app/workspace/types/workflow-common.interface";
import { mockNlpSentimentSchema } from "../../operator-metadata/mock-operator-metadata.data";
import { SchemaPropagationResponse } from "../schema-propagation/schema-propagation.service";

/**
 * Export constants related to the source table names present at the server
 */

export const mockEmptySchemaPropagationResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {},
};

export const mockAutocompleteAPIEmptyResponse: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {},
};

export const mockSentimentOperatorA: OperatorPredicate = {
  operatorID: "1",
  operatorType: mockNlpSentimentSchema.operatorType,
  operatorProperties: {},
  inputPorts: [],
  outputPorts: [{ portID: "7", displayName: "7" }],
  showAdvanced: true,
  isDisabled: false,
  operatorVersion: "1",
};

export const mockSentimentOperatorB: OperatorPredicate = {
  operatorID: "3",
  operatorType: mockNlpSentimentSchema.operatorType,
  operatorProperties: {
    attribute: "user_screen_name",
  },
  inputPorts: [{ portID: "8", displayName: "8" }],
  outputPorts: [{ portID: "9", displayName: "9" }],
  showAdvanced: true,
  isDisabled: false,
  operatorVersion: "1",
};

export const mockSentimentOperatorC: OperatorPredicate = {
  operatorID: "4",
  operatorType: mockNlpSentimentSchema.operatorType,
  operatorProperties: {
    attribute: "user_screen_name",
  },
  inputPorts: [{ portID: "10", displayName: "10" }],
  outputPorts: [],
  showAdvanced: true,
  isDisabled: false,
  operatorVersion: "1",
};

export const mockLinkAtoB: OperatorLink = {
  linkID: "link-A-to-B",
  source: {
    operatorID: mockSentimentOperatorA.operatorID,
    portID: mockSentimentOperatorA.outputPorts[0].portID,
  },
  target: {
    operatorID: mockSentimentOperatorB.operatorID,
    portID: mockSentimentOperatorB.inputPorts[0].portID,
  },
};

export const mockLinkBtoC: OperatorLink = {
  linkID: "link-B-to-C",
  source: {
    operatorID: mockSentimentOperatorB.operatorID,
    portID: mockSentimentOperatorB.outputPorts[0].portID,
  },
  target: {
    operatorID: mockSentimentOperatorC.operatorID,
    portID: mockSentimentOperatorC.inputPorts[0].portID,
  },
};

export const mockSchemaPropagationResponse1: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSentimentOperatorB.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "user_screen_name", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
  },
};

export const mockSchemaPropagationResponse2: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSentimentOperatorB.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "user_display_name", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
  },
};

export const mockSchemaPropagationResponse3: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSentimentOperatorB.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
  },
};

export const mockSchemaPropagationResponse4: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSentimentOperatorB.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "screen_display_time", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
    [mockSentimentOperatorC.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "user_screen_name", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
  },
};

export const mockSchemaPropagationResponse5: Readonly<SchemaPropagationResponse> = {
  code: 0,
  result: {
    // [var] means using the variable value as key instead of variable name
    [mockSentimentOperatorB.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "screen_display_time", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
    [mockSentimentOperatorC.operatorID]: [
      [
        { attributeName: "city", attributeType: "string" },
        { attributeName: "screen_display_time", attributeType: "string" },
        { attributeName: "user_name", attributeType: "string" },
        { attributeName: "county", attributeType: "string" },
      ],
    ],
  },
};
