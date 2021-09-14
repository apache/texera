import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { ResultTableFrameComponent } from "./result-table-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";

describe("ResultTableFrameComponent", () => {
  let component: ResultTableFrameComponent;
  let fixture: ComponentFixture<ResultTableFrameComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        imports: [HttpClientTestingModule, NzModalModule],
        declarations: [ResultTableFrameComponent],
        providers: [
          {
            provide: OperatorMetadataService,
            useClass: StubOperatorMetadataService,
          },
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("currentResult should not be modified if setupResultTable is called with empty (zero-length) execution result  ", () => {
    component.currentResult = [{ test: "property" }];
    (component as any).setupResultTable([]);

    expect(component.currentResult).toEqual([{ test: "property" }]);
  });
});
