import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SavedProjectSectionComponent } from './saved-project-section.component';

describe('SavedProjectSectionComponent', () => {
  let component: SavedProjectSectionComponent;
  let fixture: ComponentFixture<SavedProjectSectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SavedProjectSectionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SavedProjectSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
