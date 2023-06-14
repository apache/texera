import { Component, EventEmitter, Output } from "@angular/core";
import { SortMethod } from "../../type/sort-method";

@Component({
  selector: "texera-sort-button",
  templateUrl: "./sort-button.component.html",
  styleUrls: ["./sort-button.component.scss"],
})
export class SortButtonComponent {
  @Output()
  public sortMethodChange = new EventEmitter<SortMethod>();
  public sortMethod = SortMethod.EditTimeDesc;

  public lastSort(): void {
    this.sortMethod = SortMethod.EditTimeDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public dateSort(): void {
    this.sortMethod = SortMethod.CreateTimeDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.sortMethodChange.emit(this.sortMethod);
  }

  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.sortMethodChange.emit(this.sortMethod);
  }
}
