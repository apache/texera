/**
 * This file contains type declarations related to result panel data table.
 */


type TableCellMethod = (row: any) => any;

export interface TableColumn extends Readonly<{
  columnDef: string;
  header: string;
  cell: TableCellMethod;
}> { }
