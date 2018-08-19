import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductTourComponent } from './product-tour.component';

describe('ProductTourComponent', () => {
  let component: ProductTourComponent;
  let fixture: ComponentFixture<ProductTourComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProductTourComponent ]
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
