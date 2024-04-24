import { Component, Input } from "@angular/core";

@Component({
  selector: "texera-recursive-collapse",
  templateUrl: "recursive-collapse.component.html",
  styleUrls: ["recursive-collapse.component.scss"],
})
export class RecursiveCollapseComponent {
  @Input() operators: any[] = [];
}
