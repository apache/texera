import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FeatureBarComponent } from './feature-bar.component';

describe('FeatureBarComponent', () => {
  let component: FeatureBarComponent;
  let fixture: ComponentFixture<FeatureBarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FeatureBarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FeatureBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
