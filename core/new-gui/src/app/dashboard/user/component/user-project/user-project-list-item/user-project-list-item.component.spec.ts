import { ComponentFixture, TestBed } from '@angular/core/testing';
import { UserProjectListItemComponent } from './user-project-list-item.component';

describe('UserProjectListItemComponent', () => {
  let component: UserProjectListItemComponent;
  let fixture: ComponentFixture<UserProjectListItemComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UserProjectListItemComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserProjectListItemComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
