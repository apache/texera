import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserDictionary } from '../../../../type/user-dictionary';

/**
 * NgbdModalResourceViewComponent is the pop-up component to
 * let user view each dictionary. It allows user to add items
 * into a dictionary or remove a item from dictionary.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-resource-section-modal',
  templateUrl: './ngbd-modal-resource-view.component.html',
  styleUrls: ['./ngbd-modal-resource-view.component.scss', '../../../dashboard.component.scss']

})
export class NgbdModalResourceViewComponent {
  @Input() dictionary: any;
  @Output() addedName =  new EventEmitter<string>();
  @Output() deleteName =  new EventEmitter<string>();

  public name: string = '';
  public ifAdd = false;
  public removable = true;
  public visible = true;
  public selectable = true;

  constructor(public activeModal: NgbActiveModal) {}

  public onClose(): void {
    this.activeModal.close('Close');
  }

  /**
  * addKey gets the item added by user and sends it back to the main component.
  *
  * @param
  */
  public addKey(): void {

    if (this.ifAdd && this.name !== '') {
      this.addedName.emit(this.name);
      this.name = '';
    }
    this.ifAdd = !this.ifAdd;

  }

  /**
  * remove gets the item deleted by user and sends the message to the main component.
  *
  * @param item: name of the dictionary item
  */
  public remove(item: string): void {
    this.deleteName.emit(item);
  }
}


