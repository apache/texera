import {Component, OnInit} from "@angular/core";
import {ActivatedRoute} from "@angular/router";
import {UntilDestroy} from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-hub-dataset-detail",
  templateUrl: "./hub-dataset-detail.component.html",
  styleUrls: ["./hub-dataset-detail.component.scss"],
})
export class HubDatasetDetailComponent implements OnInit{
  public id: number | undefined;
  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    console.log("HubDatasetDetailComponent initialized");
    // eslint-disable-next-line rxjs-angular/prefer-takeuntil
    this.route.params.subscribe(params => {
      this.id = params["did"]
      console.log("Dataset ID:", params["did"]);
    });
  }
}
