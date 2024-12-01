import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {UntilDestroy} from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-hub-dataset-detail",
  templateUrl: "./hub-dataset-detail.component.html",
  styleUrls: ["./hub-dataset-detail.component.scss"],
})
export class HubDatasetDetailComponent{
  public id: number | undefined;
  constructor(private route: ActivatedRoute, private router: Router) {}

  ngOnInit(): void {
    console.log("HubDatasetDetailComponent initialized");
    // eslint-disable-next-line rxjs-angular/prefer-takeuntil
    this.route.params.subscribe(params => {
      this.id = params["did"]
      console.log("Dataset ID:", params["did"]);
    });

    console.log("Routes:", this.router.config);

  }
}
