import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserDictionarySectionComponent } from './user-dictionary-section.component';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../../../service/user-dictionary/stub-user-dictionary.service';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { UserDictionary } from '../../../service/user-dictionary/user-dictionary.interface';

import { HttpClientModule, HttpClient } from '@angular/common/http';
import { CustomNgMaterialModule } from '../../../../common/custom-ng-material.module';


describe('UserDictionarySectionComponent', () => {
  let component: UserDictionarySectionComponent;
  let fixture: ComponentFixture<UserDictionarySectionComponent>;

  const TestCase: UserDictionary[] = [
    {
      id: '1',
      name: 'gun control',
      items: ['gun', 'shooting'],
      description: 'This dictionary attribute to documenting the gun control records.'
    },
    {
      id: '2',
      name: 'police violence',
      items: ['BLM', 'police']
    },
    {
      id: '3',
      name: 'immigration policy',
      items: ['trump', 'daca', 'wall', 'mexico']
    }
  ];

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserDictionarySectionComponent],
      providers: [
        UserDictionaryService,
        NgbActiveModal
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule
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

  it('alphaSortTest increaseOrder', () => {
    component.userDictionaries = [];
    component.userDictionaries = component.userDictionaries.concat(TestCase);
    component.ascSort();
    const SortedCase = component.userDictionaries.map(item => item.name);
    expect(SortedCase)
      .toEqual(['gun control', 'immigration policy', 'police violence']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.userDictionaries = [];
    component.userDictionaries = component.userDictionaries.concat(TestCase);
    component.dscSort();
    const SortedCase = component.userDictionaries.map(item => item.name);
    expect(SortedCase)
      .toEqual(['police violence', 'immigration policy', 'gun control']);
  });

  it('createDateSortTest', () => {
    component.userDictionaries = [];
    component.userDictionaries = component.userDictionaries.concat(TestCase);
    component.sizeSort();
    const SortedCase = component.userDictionaries.map(item => item.name);
    expect(SortedCase)
      .toEqual(['immigration policy', 'gun control', 'police violence']);
  });

/*
* more tests of testing return value from pop-up components(windows)
* should be removed to here
*/

});
