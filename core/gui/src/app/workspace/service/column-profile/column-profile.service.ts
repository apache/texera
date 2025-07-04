import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import {
  ColumnProfile,
  TableProfile,
} from "../../../common/type/proto/edu/uci/ics/amber/engine/architecture/worker/tableprofile";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";

export interface SelectedColumnInfo {
  operatorId: string;
  columnProfile: ColumnProfile;
  tableProfile: TableProfile; // Pass the whole table profile for context (e.g. global row count)
  schema: ReadonlyArray<SchemaAttribute>;
}

@Injectable({
  providedIn: "root",
})
export class ColumnProfileService {
  private selectedColumnSubject = new BehaviorSubject<SelectedColumnInfo | null>(null);

  constructor() {}

  /**
   * Call this method to select a column for profiling.
   * This will notify all subscribers.
   */
  public selectColumn(info: SelectedColumnInfo | null): void {
    this.selectedColumnSubject.next(info);
  }

  /**
   * Get an observable stream of the currently selected column for profiling.
   */
  public getSelectedColumnStream(): Observable<SelectedColumnInfo | null> {
    return this.selectedColumnSubject.asObservable();
  }

  /**
   * Clears the currently selected column.
   */
  public clearColumnSelection(): void {
    this.selectedColumnSubject.next(null);
  }
}
