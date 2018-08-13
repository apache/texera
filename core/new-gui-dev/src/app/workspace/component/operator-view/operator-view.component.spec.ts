import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorViewComponent } from './operator-view.component';

describe('OperatorViewComponent', () => {
  let component: OperatorViewComponent;
  let fixture: ComponentFixture<OperatorViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OperatorViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
