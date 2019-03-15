import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import {MatDividerModule} from '@angular/material/divider';
import {MatDialogModule} from '@angular/material/dialog';
import {MatChipsModule} from '@angular/material/chips';
import {MatIconModule} from '@angular/material/icon';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';
import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view.component';

describe('NgbdModalResourceViewComponent', () => {
  let component: NgbdModalResourceViewComponent;
  let fixture: ComponentFixture<NgbdModalResourceViewComponent>;

  let viewcomponent: NgbdModalResourceViewComponent;
  let viewfixture: ComponentFixture<NgbdModalResourceViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceViewComponent ],
      providers: [
        NgbActiveModal
      ],
      imports: [MatDividerModule,
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
    fixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    component = fixture.componentInstance;
    // fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('resourceViewComponent addKey should generate new key', () => {
    viewfixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    viewcomponent = viewfixture.componentInstance;

    let getResult: String = '';
    viewcomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    viewcomponent.name = 'test';
    viewcomponent.ifAdd = true;
    viewcomponent.addedName.subscribe((out: any) => getResult = out);
    viewcomponent.addKey();

    expect(getResult).toEqual('test');
  });

  it('resourceViewComponent remove should indicate the key to be removed', () => {
    viewfixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    viewcomponent = viewfixture.componentInstance;

    let getRemove: String = '';
    viewcomponent.dictionary = {
      id: '1',
      name: 'police violence',
      items: ['BLM']
    };
    let item: string;
    item  = 'deleted keyword';
    viewcomponent.deleteName.subscribe((outr: any) => getRemove = outr);
    viewcomponent.remove(item);

    expect(getRemove).toEqual('deleted keyword');
  });
});
