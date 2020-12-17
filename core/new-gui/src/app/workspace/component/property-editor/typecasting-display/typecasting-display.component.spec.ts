import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TypecastingDisplayComponent } from './typecasting-display.component';

describe('TypecastingDisplayComponent', () => {
  let component: TypecastingDisplayComponent;
  let fixture: ComponentFixture<TypecastingDisplayComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TypecastingDisplayComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TypecastingDisplayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
