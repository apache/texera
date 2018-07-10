import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserDictionarySectionComponent,
  NgbdModalResourceAddComponent,
  NgbdModalResourceDeleteComponent,
  NgbdModalResourceViewComponent } from './user-dictionary-section.component';
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

import { HttpModule } from '@angular/http';
import { NgbdModalAddProjectComponent } from '../saved-project-section/saved-project-section.component';

describe('UserDictionarySectionComponent', () => {
  let component: UserDictionarySectionComponent;
  let fixture: ComponentFixture<UserDictionarySectionComponent>;

  let addcomponent: NgbdModalResourceAddComponent;
  let addfixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let viewcomponent: NgbdModalResourceViewComponent;
  let viewfixture: ComponentFixture<NgbdModalResourceViewComponent>;

  let deletecomponent: NgbdModalResourceDeleteComponent;
  let deletefixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

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
      declarations: [ UserDictionarySectionComponent,
        NgbdModalResourceAddComponent,
        NgbdModalResourceDeleteComponent,
        NgbdModalResourceViewComponent ],
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
        HttpModule]
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

  it('resourceViewComponent addKey should generate new key', () => {
    viewfixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    viewcomponent = viewfixture.componentInstance;
    // viewfixture.detectChanges();

    let getResult: String = null;
    viewcomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    viewcomponent.name = 'test';
    viewcomponent.ifAdd = true;
    viewcomponent.addedName.subscribe(out => getResult = out);
    viewcomponent.addKey();

    expect(getResult).toEqual('test');
  });

  it('resourceViewComponent remove should indicate the key to be removed', () => {
    viewfixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    viewcomponent = viewfixture.componentInstance;

    let getRemove: String = null;
    viewcomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    let item: String;
    item  = 'deleted keyword';
    viewcomponent.deleteName.subscribe(outr => getRemove = outr);
    viewcomponent.remove(item);

    expect(getRemove).toEqual('deleted keyword');
  });

  it('resourceAddComponent addKey should add a new dictionary', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    // addfixture.detectChanges();

    let getResultDict: UserDictionary = null;

    addcomponent.dictContent = 'key1,key2,key3';
    addcomponent.name = 'test';
    addcomponent.separator = ',';
    addcomponent.addedDictionary.subscribe(outd => getResultDict = outd);
    addcomponent.addKey();

    expect(getResultDict.id).toEqual('1');
    expect(getResultDict.name).toEqual('test');
    expect(getResultDict.items).toEqual(['key1', 'key2', 'key3']);
  });

  it('resourceDeleteComponent deleteDictionary should delete a certain dictionary', () => {
    deletefixture = TestBed.createComponent(NgbdModalResourceDeleteComponent);
    deletecomponent = deletefixture.componentInstance;
    // deletefixture.detectChanges();

    deletecomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    let deleteSignal: Boolean = false;
    deletecomponent.deleteDict.subscribe(outb => deleteSignal = outb);
    deletecomponent.deleteDictionary();

    expect(deleteSignal).toEqual(true);
  });

});
