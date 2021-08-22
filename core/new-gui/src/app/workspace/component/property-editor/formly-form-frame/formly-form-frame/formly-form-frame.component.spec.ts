import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FormlyFormFrameComponent } from './formly-form-frame.component';

describe('FormlyFormFrameComponent', () => {
  let component: FormlyFormFrameComponent;
  let fixture: ComponentFixture<FormlyFormFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FormlyFormFrameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FormlyFormFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
