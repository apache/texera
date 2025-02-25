import { Component, Input, OnInit } from "@angular/core";
import { DatasetStagedObject } from "../../../../../../common/type/dataset-staged-object";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { untilDestroyed } from "@ngneat/until-destroy";
import { pipe } from "rxjs";

@Component({
  selector: "texera-dataset-staged-objects-list",
  templateUrl: "./user-dataset-staged-objects-list.component.html",
  styleUrls: ["./user-dataset-staged-objects-list.component.scss"],
})
export class UserDatasetStagedObjectsListComponent implements OnInit {
  @Input() did?: number; // Dataset ID, required input from parent component
  datasetStagedObjects: DatasetStagedObject[] = [];

  constructor(private datasetService: DatasetService) {}

  ngOnInit(): void {
    console.log("did: ", this.did);
    if (this.did != undefined) {
      this.datasetService.getDatasetDiff(this.did).subscribe(diffs => {
        console.log("Received dataset diff:", diffs);
        this.datasetStagedObjects = diffs;
      });
    }
  }
}
