import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorPanelComponent } from './operator-panel.component';

describe('OperatorPanelComponent', () => {
  let component: OperatorPanelComponent;
  let fixture: ComponentFixture<OperatorPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OperatorPanelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
