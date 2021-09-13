import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { ProductTourComponent } from "./product-tour.component";
import { TourNgBootstrapModule, TourService, IStepOption } from "ngx-tour-ng-bootstrap";
import { RouterTestingModule } from "@angular/router/testing";
import { marbles } from "rxjs-marbles";
import { map, tap } from "rxjs/operators";

describe("ProductTourComponent", () => {
  let component: ProductTourComponent;
  let fixture: ComponentFixture<ProductTourComponent>;
  let tourService: TourService;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        imports: [RouterTestingModule.withRoutes([]), TourNgBootstrapModule.forRoot()],
        declarations: [ProductTourComponent],
        providers: [TourService],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductTourComponent);
    component = fixture.componentInstance;
    tourService = TestBed.get(TourService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should initialize three steps and their properties according to the input passed", () => {
    tourService.initialize$.subscribe((steps: IStepOption[]) => {
      expect(steps.length).toEqual(10);
      expect(steps[0].anchorId).toEqual("texera-navigation-grid-container");
      expect(steps[0].placement).toEqual("bottom");
      expect(steps[1].anchorId).toEqual("texera-operator-panel");
      expect(steps[1].placement).toEqual("right");
    });

    const mockComponent: ProductTourComponent = new ProductTourComponent(tourService);
  });

  it(
    "should trigger a start event when the toggle() method call is execute",
    marbles(m => {
      const tourServiceStartStream = tourService.start$.pipe(map(() => "a"));
      m.hot("-a-")
        .pipe(tap(() => tourService.toggle()))
        .subscribe();
      const expectedStream = m.hot("-a-");
      m.expect(tourServiceStartStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should trigger an end event when the end() method call is executed",
    marbles(m => {
      const tourServiceEndStream = tourService.end$.pipe(map(() => "a"));
      m.hot("-a-")
        .pipe(tap(() => tourService.end()))
        .subscribe();
      const expectedStream = m.hot("-a-");
      m.expect(tourServiceEndStream).toBeObservable(expectedStream);
    })
  );
});
