import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { Router } from "@angular/router";
import { SearchService } from "../../service/search.service";
import { DatasetService } from "../../service/user-dataset/dataset.service";
import { DashboardEntry } from "../../type/dashboard-entry";
import { SortMethod } from "../../type/sort-method";
import { DashboardDataset } from "../../type/dashboard-dataset.interface";
import Fuse from "fuse.js";

@UntilDestroy()
@Component({
  selector: "texera-dataset-section",
  templateUrl: "user-dataset.component.html",
  styleUrls: ["user-dataset.component.scss"],
})
export class UserDatasetComponent implements OnInit {
  public dashboardUserDatasetEntries: ReadonlyArray<DashboardDataset> = [];
  public userDatasetSearchValue: string = "";
  public filteredDatasetNames: Array<string> = [];
  public isTyping: boolean = false;
  public fuse = new Fuse([] as ReadonlyArray<DashboardDataset>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["dataset.name"],
  });

  selectedMenu: "All Datasets" | "Your Datasets" | "Shared with you" | "Public Datasets" = "All Datasets";

  constructor(
    private userService: UserService,
    private router: Router,
    private datasetService: DatasetService,
    private searchService: SearchService
  ) {}

  ngOnInit() {
    this.reloadDashboardDatasetEntries();
  }

  public onClickOpenDatasetAddComponent(): void {
    this.router.navigate(["/dashboard/dataset/create"]);
  }

  public searchInputOnChange(value: string): void {
    this.isTyping = true;
    this.filteredDatasetNames = [];
    const datasetArray = this.dashboardUserDatasetEntries;
    datasetArray.forEach(datasetEntry => {
      if (datasetEntry.dataset.name.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
        this.filteredDatasetNames.push(datasetEntry.dataset.name);
      }
    });
  }

  private reloadDashboardDatasetEntries(): void {
    this.datasetService
      .listDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasetEntries => {
        this.dashboardUserDatasetEntries = datasetEntries;
      });
  }

  public getDatasetArray(): ReadonlyArray<DashboardDataset> {
    const datasetArray = this.dashboardUserDatasetEntries;
    let resultDatasetArray: DashboardDataset[] = [];
    if (!datasetArray) {
      return [];
    } else if (this.userDatasetSearchValue !== "" && !this.isTyping) {
      this.fuse.setCollection(datasetArray);
      resultDatasetArray = this.fuse.search(this.userDatasetSearchValue).map(item => {
        return item.item;
      });
    } else if (!this.isTyping) {
      resultDatasetArray = datasetArray.slice();
    }
    // apply the filter condition
    if (this.selectedMenu === "Your Datasets") {
      resultDatasetArray = resultDatasetArray.filter(dataset => {
        return dataset.isOwner;
      });
    } else if (this.selectedMenu === "Shared with you") {
      resultDatasetArray = resultDatasetArray.filter(dataset => {
        return !dataset.isOwner && !dataset.dataset.isPublic;
      });
    } else if (this.selectedMenu === "Public Datasets") {
      resultDatasetArray = resultDatasetArray.filter(dataset => {
        return dataset.dataset.isPublic;
      });
    }
    return resultDatasetArray;
  }

  public deleteDataset(entry: DashboardDataset) {
    if (entry.dataset.did) {
      this.datasetService
        .deleteDatasets([entry.dataset.did])
        .pipe(untilDestroyed(this))
        .subscribe(_ => {
          this.reloadDashboardDatasetEntries();
        });
    }
  }
}
