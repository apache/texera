import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HubDatasetResultComponent } from './hub-dataset-result.component';

describe('HubDatasetResultComponent', () => {
  let component: HubDatasetResultComponent;
  let fixture: ComponentFixture<HubDatasetResultComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [HubDatasetResultComponent]
    });
    fixture = TestBed.createComponent(HubDatasetResultComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
