import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FeatureContainerComponent } from './feature-container.component';

describe('FeatureContainerComponent', () => {
  let component: FeatureContainerComponent;
  let fixture: ComponentFixture<FeatureContainerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FeatureContainerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FeatureContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
