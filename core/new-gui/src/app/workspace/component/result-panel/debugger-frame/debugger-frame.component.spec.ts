import { ComponentFixture, TestBed } from "@angular/core/testing";

import { DebuggerFrameComponent } from "./debugger-frame.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";

describe("DebuggerFrameComponent", () => {
  let component: DebuggerFrameComponent;
  let fixture: ComponentFixture<DebuggerFrameComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [DebuggerFrameComponent],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DebuggerFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
