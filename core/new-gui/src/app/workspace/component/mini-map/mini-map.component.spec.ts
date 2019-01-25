import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MiniMapComponent } from './mini-map.component';

describe('MiniMapComponent', () => {
  let component: MiniMapComponent;
  let fixture: ComponentFixture<MiniMapComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MiniMapComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
