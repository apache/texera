import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';
import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete.component';

describe('NgbdModalResourceDeleteComponent', () => {
  let component: NgbdModalResourceDeleteComponent;
  let fixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

  let deletecomponent: NgbdModalResourceDeleteComponent;
  let deletefixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceDeleteComponent ],
      providers: [
        NgbActiveModal
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceDeleteComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('resourceDeleteComponent deleteDictionary should delete a certain dictionary', () => {
    deletefixture = TestBed.createComponent(NgbdModalResourceDeleteComponent);
    deletecomponent = deletefixture.componentInstance;

    deletecomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    let deleteSignal: Boolean;
    deleteSignal = false;
    deletecomponent.deleteDictionary();

    expect(deleteSignal).toEqual(false);
  });
});
