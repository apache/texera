import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';

describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PropertyEditorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
