import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserDictionarySectionComponent } from './user-dictionary-section.component';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../../../service/user-dictionary/stub-user-dictionary.service';

import {MatDividerModule} from '@angular/material/divider';
import {MatListModule} from '@angular/material/list';
import {MatCardModule} from '@angular/material/card';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatDialogModule} from '@angular/material/dialog';
import {MatChipsModule} from '@angular/material/chips';
import {MatIconModule} from '@angular/material/icon';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { UserDictionary } from '../../../type/user-dictionary';

import { HttpClientModule, HttpClient } from '@angular/common/http';


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
        { provide: UserDictionaryService, useClass: StubUserDictionaryService },
        NgbActiveModal
      ],
      imports: [MatCardModule,
        MatDividerModule,
        MatListModule,
        MatFormFieldModule,
        MatDialogModule,
        MatChipsModule,
        MatIconModule,

        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule]
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
    component.UserDictionary = [];
    component.UserDictionary = component.UserDictionary.concat(TestCase);
    component.ascSort();
    const SortedCase = component.UserDictionary.map(item => item.name);
    expect(SortedCase)
      .toEqual(['gun control', 'immigration policy', 'police violence']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.UserDictionary = [];
    component.UserDictionary = component.UserDictionary.concat(TestCase);
    component.dscSort();
    const SortedCase = component.UserDictionary.map(item => item.name);
    expect(SortedCase)
      .toEqual(['police violence', 'immigration policy', 'gun control']);
  });

  it('createDateSortTest', () => {
    component.UserDictionary = [];
    component.UserDictionary = component.UserDictionary.concat(TestCase);
    component.sizeSort();
    const SortedCase = component.UserDictionary.map(item => item.name);
    expect(SortedCase)
      .toEqual(['immigration policy', 'gun control', 'police violence']);
  });

/*
* more tests of testing return value from pop-up components(windows)
* should be removed to here
*/

});
