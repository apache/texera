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
export interface IndexableObject
  extends Readonly<{
    [key: string]: object | string | boolean | symbol | number | Array<object>;
  }> {}

/**
 * This type represent the function type interface for
 *  retreiving each attribute from each result row.
 * Given a row, extract the cell value of each column.
 */
type TableCellMethod = (row: IndexableObject) => object | string | number | boolean;

/**
 * TableColumn specifies the information about each column.
 * It has:
 *  - columnDef - the value to reference that column
 *  - header - the header of that column, which is the text to be displayed on the GUI
 *  - getCell - a function that returns the cell value that will be dispalyed in each cell of the data table
 */
export interface TableColumn
  extends Readonly<{
    columnDef: string;
    header: string;
    getCell: TableCellMethod;
  }> {}

export const PAGINATION_INFO_STORAGE_KEY = "result-panel-pagination-info";

export interface ViewResultOperatorInfo
  extends Readonly<{
    currentResult: object[];
    currentPageIndex: number;
    total: number;
    columnKeys: string[];
    operatorID: string;
  }> {}

/**
 * ResultPaginationInfo stores pagination information
 *   that is needed for status retainment of the result panel
 */
export interface ResultPaginationInfo
  extends Readonly<{
    newWorkflowExecuted: boolean;
    viewResultOperatorInfoMap: Map<string, ViewResultOperatorInfo>;
  }> {}
