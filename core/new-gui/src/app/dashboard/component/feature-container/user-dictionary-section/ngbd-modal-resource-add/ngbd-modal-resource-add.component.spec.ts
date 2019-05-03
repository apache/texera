import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import {MatDividerModule} from '@angular/material/divider';
import {MatDialogModule} from '@angular/material/dialog';
import {MatFormFieldModule} from '@angular/material/form-field';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';

import { UserDictionary } from '../../../../type/user-dictionary';

describe('NgbdModalResourceAddComponent', () => {
  let component: NgbdModalResourceAddComponent;
  let fixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let addcomponent: NgbdModalResourceAddComponent;
  let addfixture: ComponentFixture<NgbdModalResourceAddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceAddComponent ],
      providers: [
        NgbActiveModal
      ],
      imports: [MatDividerModule,
        MatFormFieldModule,
        MatDialogModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('resourceAddComponent addDictionary should add a new dictionary', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;

    let getResultDict: UserDictionary;
    getResultDict = {
      id: '1',
      name: 'test',
      items: [],
    };

    addcomponent.dictContent = 'key1,key2,key3';
    addcomponent.name = 'test';
    addcomponent.separator = ',';
    addcomponent.addDictionary();

    expect(getResultDict.id).toEqual('1');
    expect(getResultDict.name).toEqual('test');
    expect(getResultDict.items).toEqual([]);
  });
});
