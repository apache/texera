declare module 'hocon-parser' {
  /**
   * The module itself is callable, accepting a string (HOCON config) and returning a parsed object.
   */
  function hoconParser(input: string): any;

  export = hoconParser;
}