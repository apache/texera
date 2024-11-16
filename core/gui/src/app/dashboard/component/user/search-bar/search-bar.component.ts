import { Component } from "@angular/core";
import { Router } from "@angular/router";
import { SearchService } from "../../../service/user/search.service";
import { SearchFilterParameters } from "../../../type/search-filter-parameters";
import { SortMethod } from "../../../type/sort-method";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SearchResult, SearchResultItem } from "../../../type/search-result";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { Subject, Observable, of } from "rxjs";
import { debounceTime, switchMap } from "rxjs/operators";
import { UserService } from "../../../../common/service/user/user.service";
import { DASHBOARD_SEARCH } from "../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-search-bar",
  templateUrl: "./search-bar.component.html",
  styleUrls: ["./search-bar.component.scss"],
})
export class SearchBarComponent {
  private includePublic = true;
  public searchParam: string = "";
  public listOfResult: string[] = [];
  private searchSubject = new Subject<string>();
  isLogin = this.userService.isLogin();

  private params: SearchFilterParameters = {
    createDateStart: null,
    createDateEnd: null,
    modifiedDateStart: null,
    modifiedDateEnd: null,
    owners: [],
    ids: [],
    operators: [],
    projectIds: [],
  };

  private searchCache = new Map<string, string[]>();
  private queryOrder: string[] = [];

  constructor(
    private router: Router,
    private searchService: SearchService,
    private userService: UserService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
      });
    this.searchSubject
      .pipe(
        debounceTime(200),
        switchMap(query => this.getSearchResults(query)),
        untilDestroyed(this)
      )
      .subscribe((results: string[]) => {
        this.listOfResult = results;
      });
  }

  // Method to get search results with caching and limit cache size
  private getSearchResults(query: string): Observable<string[]> {
    if (this.searchCache.has(query)) {
      return of(this.searchCache.get(query)!);
    } else {
      const searchObservable = this.searchService.search(
        [query],
        this.params,
        0,
        5,
        null,
        SortMethod.NameAsc,
        this.isLogin,
        this.includePublic
      );

      return searchObservable.pipe(
        switchMap((result: SearchResult) => {
          const uniqueResults = Array.from(new Set(result.results.map(item => this.convertToName(item))));
          this.addToCache(query, uniqueResults);
          return of(uniqueResults);
        })
      );
    }
  }

  private addToCache(query: string, results: string[]): void {
    if (this.queryOrder.length >= 20) {
      const oldestQuery = this.queryOrder.shift();
      this.searchCache.delete(oldestQuery!);
    }
    this.queryOrder.push(query);
    this.searchCache.set(query, results);
  }

  onSearchInputChange(query: string): void {
    if (query) {
      this.searchSubject.next(query);
    } else {
      this.listOfResult = [];
    }
  }

  performSearch(keyword: string) {
    this.router.navigate([DASHBOARD_SEARCH], { queryParams: { q: keyword } });
  }

  convertToName(resultItem: SearchResultItem): string {
    if (resultItem.workflow) {
      return new DashboardEntry(resultItem.workflow).name;
    } else if (resultItem.project) {
      return new DashboardEntry(resultItem.project).name;
    } else if (resultItem.file) {
      return new DashboardEntry(resultItem.file).name;
    } else if (resultItem.dataset) {
      return new DashboardEntry(resultItem.dataset).name;
    } else {
      throw new Error("Unexpected type in SearchResult.");
    }
  }
}
