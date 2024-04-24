import { Component, Input } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-recursive-collapse",
  templateUrl: "recursive-collapse.component.html",
  styleUrls: ["recursive-collapse.component.scss"],
})
export class RecursiveCollapseComponent {
  @Input() operators: any[] = [];
}
