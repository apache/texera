import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { ConsoleFrameComponent } from "./console-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("ConsoleFrameComponent", () => {
  let component: ConsoleFrameComponent;
  let fixture: ComponentFixture<ConsoleFrameComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzDropDownModule],
      declarations: [ConsoleFrameComponent],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConsoleFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
