/**
 * This file contains type declarations related to result panel data table.
 */


/**
 * Since only type `any` is indexable in typescript, as shown in
 *  https://basarat.gitbooks.io/typescript/docs/types/index-signatures.html,
 *  we need to explicitly define an `Indexable Types` described in
 *  https://www.typescriptlang.org/docs/handbook/interfaces.html
 *  to make `row` indexable and execute operation like `row[col]`.
 */
export interface IndexableObject extends Readonly<{
  [key: string]: object;
}> { }

/**
 * This type represent the function type interface for
 *  retreiving each attribute from each result row.
 *  Given a row, extract the value of each column, which
 *  can be type object, string, number, and boolean.
 */
type TableCellMethod = (row: IndexableObject) => object | string | number | boolean;

/**
 * TableColumn specifies the information about each column.
 *  It stores the columnDef, which is the type of the value that
 *  one column stores. Header is the header of that column, which
 *  will be displayed on the GUI. Cell is a function that returns
 *  the value that will be dispalyed in each cell of the data table.
 */
export interface TableColumn extends Readonly<{
  columnDef: string;
  header: string;
  cell: TableCellMethod;
}> { }
