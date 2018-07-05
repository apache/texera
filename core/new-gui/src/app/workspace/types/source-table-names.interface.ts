/**
 * This file contain the type decalaration of the response sent by **backend** when
 * asking for source table names.
 */

export interface SourceTableNamesAPIResponse extends Readonly < {
  code: number,
  message: string
} > { }

export interface SourceTableDetails extends Readonly <{
  tableName: string,
  schema: SourceTableSchema
}> { }

export interface SourceTableSchema extends Readonly<{
  attributes: ReadonlyArray<SourceTableAttribute>
}> { }

export interface SourceTableAttribute extends Readonly <{
  attributeName: string,
  attributeType: string
}> { }
