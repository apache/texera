import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductTourComponent } from './product-tour.component';

import { TourNgBootstrapModule, TourService } from 'ngx-tour-ng-bootstrap';

import { RouterTestingModule } from '@angular/router/testing';

describe('ProductTourComponent', () => {
  let component: ProductTourComponent;
  let fixture: ComponentFixture<ProductTourComponent>;

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
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
