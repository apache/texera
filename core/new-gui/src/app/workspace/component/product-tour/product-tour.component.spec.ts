import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductTourComponent } from './product-tour.component';

import { TourNgBootstrapModule, TourService, IStepOption } from 'ngx-tour-ng-bootstrap';

import { RouterTestingModule } from '@angular/router/testing';

import { mockTourSteps } from '../../service/product-tour/mock-product-tour.data';

import { marbles } from 'rxjs-marbles';


describe('ProductTourComponent', () => {
  let component: ProductTourComponent;
  let fixture: ComponentFixture<ProductTourComponent>;
  let tourService: TourService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ RouterTestingModule.withRoutes([]), TourNgBootstrapModule.forRoot() ],
      declarations: [ ProductTourComponent ],
      providers: [ TourService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductTourComponent);
    component = fixture.componentInstance;
    tourService = TestBed.get(TourService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize three steps and their properties according to the input passed', () => {
    const mockdata = mockTourSteps;
    tourService.initialize$.subscribe((steps: IStepOption[]) => {
      expect(steps.length).toEqual(3);
      expect(steps[0].anchorId).toEqual('texera-navigation-grid-container');
      expect(steps[0].placement).toEqual('bottom');
      expect(steps[1].anchorId).toEqual('texera-operator-panel');
      expect(steps[1].placement).toEqual('right');
      expect(steps[2].anchorId).toEqual('texera-operator-panel');
      expect(steps[2].placement).toEqual('right');
    });
    tourService.initialize(mockdata);
  });

  // // not working currently because tourService toggle() cannot find the [tourAnchor] attribute
  // it('should execute the previous step and hide the current step if tourService.prev() is called', () => {
  //   const mockdata = mockTourSteps;
  //   tourService.initialize(mockdata);
  //   tourService.toggle();
  //   tourService.next();
  //   tourService.stepShow$.subscribe((steps: IStepOption) => {
  //     expect(steps.title).toEqual('Step One');
  //   });
  //   tourService.stepHide$.subscribe((steps: IStepOption) => {
  //     expect(steps.title).toEqual('Step Two');
  //   });
  //   tourService.prev();
  // });

  it('should trigger a start event when the toggle() method call is execute', marbles((m) => {
    const tourServiceStartStream = tourService.start$.map(() => 'a');
    m.hot('-a-').do(() => tourService.toggle()).subscribe();
    const expectedStream = m.hot('-a-');
    m.expect(tourServiceStartStream).toBeObservable(expectedStream);
  }));

  it('should trigger an end event when the end() method call is executed', marbles((m) => {
    const tourServiceEndStream = tourService.end$.map(() => 'a');
        // change this tourService.end() call to html element triggerEvent(click, null) once toggle problem is resolved
    m.hot('-a-').do(() => tourService.end()).subscribe();
    const expectedStream = m.hot('-a-');
    m.expect(tourServiceEndStream).toBeObservable(expectedStream);
  }));

});
