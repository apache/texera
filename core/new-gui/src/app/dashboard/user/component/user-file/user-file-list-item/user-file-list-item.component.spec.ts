import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UserFileListItemComponent } from './user-file-list-item.component';

describe('UserFileListItemComponent', () => {
  let component: UserFileListItemComponent;
  let fixture: ComponentFixture<UserFileListItemComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UserFileListItemComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserFileListItemComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
