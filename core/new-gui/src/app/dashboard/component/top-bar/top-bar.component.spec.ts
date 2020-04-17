import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TopBarComponent } from './top-bar.component';
import { UserAccountIconComponent } from './user-account-icon/user-account-icon.component';
import { RouterTestingModule } from '@angular/router/testing';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';

describe('TopBarComponent', () => {
  let component: TopBarComponent;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TopBarComponent, UserAccountIconComponent ],
      imports: [ RouterTestingModule, CustomNgMaterialModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    const fixture = TestBed.createComponent(TopBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
