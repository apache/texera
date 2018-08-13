import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';


@Injectable()
export class TableAutocompleteService {

  constructor(
    private httpClient: HttpClient
  ) { }

  public fetchAllTableMetadata() {

  }

}
