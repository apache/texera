import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductTourComponent } from './product-tour.component';

import { TourNgBootstrapModule, TourService, IStepOption } from 'ngx-tour-ng-bootstrap';

import { RouterTestingModule } from '@angular/router/testing';

import { mockTourSteps } from '../../service/product-tour/mock-product-tour.data';

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

  it('should initialize three steps', () => {
    const mockdata = mockTourSteps;
    tourService.initialize(mockdata);
    tourService.initialize$.subscribe((steps: IStepOption[]) => {
      expect(steps.length).toEqual(3);
    });
  });

  it('should execute next step', () => {
    const mockdata = mockTourSteps;
    tourService.initialize(mockdata);
    tourService.next();
    tourService.stepShow$.subscribe((steps: IStepOption) => {
      expect(steps.title).toEqual('Step Two');
    });
    tourService.stepHide$.subscribe((steps: IStepOption) => {
      expect(steps.title).toEqual('Step One');
    });
  });

  it('should execute previous step', () => {
    const mockdata = mockTourSteps;
    tourService.initialize(mockdata);
    tourService.next();
    tourService.prev();
    tourService.stepShow$.subscribe((steps: IStepOption) => {
      expect(steps.title).toEqual('Step One');
    });
    tourService.stepHide$.subscribe((steps: IStepOption) => {
      expect(steps.title).toEqual('Step Two');
    });
  });

  it('should execute tour end', () => {
    const mockdata = mockTourSteps;
    tourService.initialize(mockdata);
    tourService.end();
    tourService.end$.subscribe((steps: any) => {
      expect(steps).toBeUndefined;
    });
  });

});
