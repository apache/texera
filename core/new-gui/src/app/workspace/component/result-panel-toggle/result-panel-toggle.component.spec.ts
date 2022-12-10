import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { ResultPanelToggleComponent } from "./result-panel-toggle.component";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";

describe("ResultPanelToggleComponent", () => {
  let component: ResultPanelToggleComponent;
  let fixture: ComponentFixture<ResultPanelToggleComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [ResultPanelToggleComponent],
        providers: [ResultPanelToggleService],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelToggleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
