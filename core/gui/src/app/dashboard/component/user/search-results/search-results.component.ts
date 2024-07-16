import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";

export type LoadMoreFunction = (start: number, count: number) => Promise<{ entries: DashboardEntry[]; more: boolean }>;

@Component({
  selector: "texera-search-results",
  templateUrl: "./search-results.component.html",
  styleUrls: ["./search-results.component.scss"],
})
export class SearchResultsComponent {
  loadMoreFunction: LoadMoreFunction | null = null;
  loading = false;
  more = false;
  entries: ReadonlyArray<DashboardEntry> = [];
  private resetCounter = 0;
  @Input() showResourceTypes = false;
  @Input() public pid: number = 0;
  @Input() editable = false;
  @Input() searchKeywords: string[] = [];
  @Output() deleted = new EventEmitter<DashboardEntry>();
  @Output() duplicated = new EventEmitter<DashboardEntry>();
  @Output() modified = new EventEmitter<DashboardEntry>();

  constructor(private userService: UserService) {}

  getUid(): number | undefined {
    return this.userService.getCurrentUser()?.uid;
  }

  reset(loadMoreFunction: LoadMoreFunction): void {
    this.entries = [];
    this.loadMoreFunction = loadMoreFunction;
    this.resetCounter++;
  }

  async loadMore(): Promise<void> {
    if (!this.loadMoreFunction) {
      throw new Error("This is an empty list and cannot load more entries.");
    }
    this.loading = true;
    try {
      const originalResetCounter = this.resetCounter;
      const results = await this.loadMoreFunction(this.entries.length, 20);
      if (this.resetCounter !== originalResetCounter) {
        return;
      }
      this.entries = [...this.entries, ...results.entries];
      this.more = results.more;
    } finally {
      this.loading = false;
    }
  }
}
