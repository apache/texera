import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

// sub component for view-dictionary popup window
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

  onClose() {
    this.activeModal.close('Close');
  }

  addKey() {

    if (this.ifAdd && this.name !== '') {
      this.addedName.emit(this.name);
      this.name = '';
    }
    this.ifAdd = !this.ifAdd;

  }

  remove(item: any): void {
    this.deleteName.emit(item);
  }
}


