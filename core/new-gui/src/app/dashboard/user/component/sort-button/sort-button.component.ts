import { Component, EventEmitter, Input, Output } from "@angular/core";
import { SortMethod } from "../../type/sort-method";
import { DashboardEntry } from "../../type/dashboard-entry";

@Component({
  selector: "texera-sort-button",
  templateUrl: "./sort-button.component.html",
  styleUrls: ["./sort-button.component.scss"],
})
export class SortButtonComponent {
  public sortMethod = SortMethod.EditTimeDesc;
  _entries?: ReadonlyArray<DashboardEntry>;
  @Input() get entries(): ReadonlyArray<DashboardEntry> {
    if (!this._entries) {
      throw new Error("entries property must be set for SortButtonComponent.");
    }
    return this._entries;
  }
  set entries(value: ReadonlyArray<DashboardEntry>) {
    const update = () => {
      this._entries = value;
      this.entriesChange.emit(value);
    };
    // Update entries property only if the input differ from existing value. This breaks the infinite recursion.
    if (this._entries === undefined || value.length != this._entries.length) {
      update();
    }
    for (let i = 0; i < value.length; i++) {
      if (value[i] != this.entries[i]) {
        update();
        return;
      }
    }
  }

  @Output() entriesChange = new EventEmitter<typeof this.entries>();

  /**
   * Sort the workflows according to the sortMethod variable
   */
  public sort(): void {
    switch (this.sortMethod) {
      case SortMethod.NameAsc:
        this.ascSort();
        break;
      case SortMethod.NameDesc:
        this.dscSort();
        break;
      case SortMethod.EditTimeDesc:
        this.lastSort();
        break;
      case SortMethod.CreateTimeDesc:
        this.dateSort();
        break;
    }
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.entries = this.entries.slice().sort((t1, t2) => {
      if (t1.workflow && t2.workflow)
        return t1.workflow.name.toLowerCase().localeCompare(t2.workflow.name.toLowerCase());
      else throw new Error("No sortable entry provided.");
    });
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.entries = this.entries.slice().sort((t1, t2) => {
      if (t1.workflow && t2.workflow)
        return t2.workflow.name.toLowerCase().localeCompare(t1.workflow.name.toLowerCase());
      else throw new Error("No sortable entry provided.");
    });
  }

  /**
   * sort the project by creating time in descending order
   */
  public dateSort(): void {
    this.sortMethod = SortMethod.CreateTimeDesc;
    this.entries = this.entries.slice().sort((t1, t2) => {
      if (t1.workflow && t2.workflow)
        return t1.workflow.creationTime !== undefined && t2.workflow.creationTime !== undefined
          ? t2.workflow.creationTime - t1.workflow.creationTime
          : 0;
      else throw new Error("No sortable entry provided.");
    });
  }

  /**
   * sort the project by last modified time in descending order
   */
  public lastSort(): void {
    this.sortMethod = SortMethod.EditTimeDesc;
    this.entries = this.entries.slice().sort((t1, t2) => {
      if (t1.workflow && t2.workflow)
        return t1.workflow.lastModifiedTime !== undefined && t2.workflow.lastModifiedTime !== undefined
          ? t2.workflow.lastModifiedTime - t1.workflow.lastModifiedTime
          : 0;
      else throw new Error("No sortable entry provided.");
    });
  }
}
