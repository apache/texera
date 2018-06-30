import { Injectable } from '@angular/core';
import HttpClient from '@angular/common/http';

export SOURCE_TABLE_NAMES_ENDPOINT = '/resources/table-metadata';
export EMPTY_TABLE_NAME = '';
/**
 * Source Table Names service is designed to request the backend for the names of the tables present at the server.
 * This list of names will be provided as a property of the source operators which need a table name like
 * source:scan, source:keyword etc.
 *
 * @author Avinash Kumar
 */
@Injectable()
export class SourceTableNamesService {

  private sourceTables: ReadonlyArray<String> | undefined;

  private sourceTablesObservable = this.httpClient
  .get;

  constructor(private httpClient: HttpClient) { }

}
