import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResourceSectionComponent } from './resource-section.component';

describe('ResourceSectionComponent', () => {
  let component: ResourceSectionComponent;
  let fixture: ComponentFixture<ResourceSectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResourceSectionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
