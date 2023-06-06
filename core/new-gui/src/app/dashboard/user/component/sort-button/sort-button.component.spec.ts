import { ComponentFixture, TestBed } from "@angular/core/testing";

import { SortButtonComponent } from "./sort-button.component";
import { testWorkflowEntries } from "../user-workflow/user-workflow-test-fixtures";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("SortButtonComponent", () => {
  let component: SortButtonComponent;
  let fixture: ComponentFixture<SortButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SortButtonComponent],
      imports: [NzDropDownModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SortButtonComponent);
    component = fixture.componentInstance;
    component.entries = [...testWorkflowEntries];
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("alphaSortTest increaseOrder", () => {
    component.ascSort();
    const SortedCase = component.entries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  });

  it("alphaSortTest decreaseOrder", () => {
    component.dscSort();
    const SortedCase = component.entries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 5", "workflow 4", "workflow 3", "workflow 2", "workflow 1"]);
  });

  it("createDateSortTest", () => {
    component.dateSort();
    const SortedCase = component.entries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5", "workflow 2", "workflow 3", "workflow 1"]);
  });

  it("lastEditSortTest", () => {
    component.lastSort();
    const SortedCase = component.entries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 5", "workflow 4", "workflow 3", "workflow 2", "workflow 1"]);
  });
});
