import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserDictionarySectionComponent } from './user-dictionary-section.component';
import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../../../service/user-dictionary/stub-user-dictionary.service';

describe('UserDictionarySectionComponent', () => {
  let component: UserDictionarySectionComponent;
  let fixture: ComponentFixture<UserDictionarySectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserDictionarySectionComponent ],
      providers: [
        { provide: UserDictionaryService, useClass: StubUserDictionaryService }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserDictionarySectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
