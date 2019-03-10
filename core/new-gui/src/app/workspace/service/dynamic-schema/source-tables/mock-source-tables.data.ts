import { SourceTableResponse, SourceTableDetail } from './source-tables.service';
import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { OperatorSchema } from '../../../types/operator-schema.interface';

// constants related to the source table names present at the server

export const mockTablePromed: SourceTableDetail = {
  tableName: 'promed',
  schema: {
    attributes: [
      {
        attributeName: '_id',
        attributeType: '_id'
      },
      {
        attributeName: 'id',
        attributeType: 'string'
      },
      {
        attributeName: 'content',
        attributeType: 'text'
      }
    ]
  }
};

export const mockTableTwitter: SourceTableDetail = {
  tableName: 'twitter_sample',
  schema: {
    'attributes': [
      {
        attributeName: '_id',
        attributeType: '_id'
      },
      {
        attributeName: 'text',
        attributeType: 'text'
      },
      {
        attributeName: 'tweet_link',
        attributeType: 'string'
      },
      {
        attributeName: 'user_link',
        attributeType: 'string'
      },
      {
        attributeName: 'user_screen_name',
        attributeType: 'text'
      },
      {
        attributeName: 'user_name',
        attributeType: 'text'
      },
      {
        attributeName: 'user_description',
        attributeType: 'text'
      },
      {
        attributeName: 'user_followers_count',
        attributeType: 'integer'
      },
      {
        attributeName: 'user_friends_count',
        attributeType: 'integer'
      },
      {
        attributeName: 'state',
        attributeType: 'text'
      },
      {
        attributeName: 'county',
        attributeType: 'text'
      },
      {
        attributeName: 'city',
        attributeType: 'text'
      },
      {
        attributeName: 'create_at',
        attributeType: 'string'
      }
    ]
  }
};


// TODO: All responses from backend should come as a JSON object. The JSON message below will
// have to be changed to be a JSON object. Same change will need to be done on the backend.
export const mockSourceTableAPIResponse: Readonly<SourceTableResponse> = {
  code: 0,
  message: JSON.stringify([mockTablePromed, mockTableTwitter])
};
