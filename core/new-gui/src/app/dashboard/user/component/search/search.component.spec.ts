import { ComponentFixture, TestBed } from "@angular/core/testing";
import { SearchComponent } from "./search.component";
import { SearchService } from "../../service/search.service";
import { StubSearchService } from "../../service/stub-search.service";
import { testUserProjects, testWorkflowEntries } from "../user-dashboard-test-fixtures";
import { FiltersComponent } from "../filters/filters.component";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { StubUserProjectService } from "../../service/user-project/stub-user-project.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzListModule } from "ng-zorro-antd/list";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzSelectModule } from "ng-zorro-antd/select";
import { ScrollingModule } from "@angular/cdk/scrolling";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { DashboardEntry } from "../../type/dashboard-entry";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";

describe("SearchComponent", () => {
  let component: SearchComponent;
  let fixture: ComponentFixture<SearchComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SearchComponent, FiltersComponent, SearchResultsComponent],
      providers: [
        {
          provide: SearchService,
          useValue: new StubSearchService([
            ...testWorkflowEntries,
            ...testUserProjects.map(i => new DashboardEntry(i)),
          ]),
        },
        { provide: UserService, useClass: StubUserService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserProjectService, useClass: StubUserProjectService },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
      ],
      imports: [
        NzDropDownModule,
        NzCardModule,
        NzListModule,
        NzButtonModule,
        NzSelectModule,
        ScrollingModule,
        NoopAnimationsModule,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("searches workflows", async () => {
    component.filters.masterFilterList = [testWorkflowEntries[0].name];
    fixture.detectChanges();
    await fixture.whenStable();
    if (!component.searchResultsComponent) {
      throw new Error("searchResultsComponent is null.");
    }
    expect(component.searchResultsComponent.entries.map(i => i.name)).toEqual([testWorkflowEntries[0].name]);
  });

  it("searches projects", async () => {
    component.filters.masterFilterList = [testUserProjects[0].name];
    fixture.detectChanges();
    await fixture.whenStable();
    if (!component.searchResultsComponent) {
      throw new Error("searchResultsComponent is null.");
    }
    expect(component.searchResultsComponent.entries.map(i => i.name)).toEqual([testUserProjects[0].name]);
  });

  it("searches workflows and projects", async () => {
    component.filters.masterFilterList = ["1"];
    fixture.detectChanges();
    await fixture.whenStable();
    if (!component.searchResultsComponent) {
      throw new Error("searchResultsComponent is null.");
    }
    const names = component.searchResultsComponent.entries.map(i => i.name);
    expect(names).toContain(testWorkflowEntries[0].name);
    expect(names).toContain(testUserProjects[0].name);
    expect(component.searchResultsComponent.entries.length).toEqual(2);
  });
});
