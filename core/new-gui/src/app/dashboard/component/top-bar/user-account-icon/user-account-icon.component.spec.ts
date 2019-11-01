import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserAccountIconComponent } from './user-account-icon.component';

describe('UserAccountIconComponent', () => {
  let component: UserAccountIconComponent;
  let fixture: ComponentFixture<UserAccountIconComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserAccountIconComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserAccountIconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
