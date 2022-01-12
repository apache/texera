/**
 * make sure do not add const/declare before enum here.
 * Const enums are removed during transpiration in JS so you can not use them at runtime.
 * Source: https://stackoverflow.com/questions/50365598/typescript-runtime-error-cannot-read-property-of-undefined-enum
 */
export enum GenericWebResponseCode {
  SUCCESS = 0,
}

export interface GenericWebResponse
  extends Readonly<{
    code: GenericWebResponseCode;
    message: string;
  }> {}
