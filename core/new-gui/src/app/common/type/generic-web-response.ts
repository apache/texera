export interface GenericWebResponse extends Readonly<{
  code: number;
  message: string;
}> {}

export declare enum GenericWebResponseCode {
  SUCCESS = 0
}
