import { Component } from "@angular/core";
import { Router } from "@angular/router";

@Component({
  selector: "texera-search-bar",
  templateUrl: "./search-bar.component.html",
  styleUrls: ["./search-bar.component.scss"],
})
export class SearchBarComponent {
  public searchParam: string = "";

  constructor(private router: Router) {}

  performSearch(keyword: string) {
    this.router.navigate(["/dashboard/search"], { queryParams: { q: keyword } });
  }
}
