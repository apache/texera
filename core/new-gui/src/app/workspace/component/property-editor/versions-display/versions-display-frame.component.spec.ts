import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { VersionsDisplayFrameComponent } from "./versions-display-frame.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("VersionsListDisplayComponent", () => {
  let component: VersionsDisplayFrameComponent;
  let fixture: ComponentFixture<VersionsDisplayFrameComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [VersionsDisplayFrameComponent],
      providers: [],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VersionsDisplayFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
