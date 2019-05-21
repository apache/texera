import { Component } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary } from '../../../../service/user-dictionary/user-dictionary.interface';
import { Observable } from '../../../../../../../node_modules/rxjs';
import { v4 as uuid } from 'uuid';

/**
 * NgbdModalResourceAddComponent is the pop-up component to let
 * user upload dictionary. User can either input the dictionary
 * name and items or drag and drop the dictionary file from
 * local computer.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-resource-section-add-dict-modal',
  templateUrl: 'ngbd-modal-resource-add.component.html',
  styleUrls: ['./ngbd-modal-resource-add.component.scss', '../../../dashboard.component.scss'],
  providers: [
    UserDictionaryService,
  ]
})
export class NgbdModalResourceAddComponent {

  public newDictionary: UserDictionary | undefined; // potential issue
  public name: string = '';
  public dictContent: string = '';
  public separator: string = '';
  public dictionaryDescription: string = '';
  public selectFile: any = null; // potential issue

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) {}

  public onChange(event: any): void {
    this.selectFile = event.target.files[0];
  }


  /**
  * addDictionary records the new dictionary information and sends
  *   it to the backend. This method will check if the user
  *   upload dictionary files first. If not, the method will read the
  *   dictionary information from the input form.
  *
  * @param
  */
  public addDictionary(): void {

    if (this.selectFile !== null) {
      this.activeModal.close(this.userDictionaryService.uploadDictionary(this.selectFile));
      return;
    }

    if (this.name !== '' && this.dictContent !== '' && this.separator !== '') {
      this.newDictionary = {
        id : this.getDictionaryRandomID(),
        name : this.name,
        items : [],
        description: this.dictionaryDescription
      };

      const listWithDup = this.dictContent.trim().split(this.separator)
        .map(dictItem => dictItem.trim()).filter(item => item.length !== 0);


      this.newDictionary.items = listWithDup.filter((v, i) => listWithDup.indexOf(v) === i);

      this.resetModal();

      this.activeModal.close(this.userDictionaryService.putUserDictionaryData(this.newDictionary));
      return;
    }

    this.activeModal.close(Observable.empty());

  }

  /**
   * Reset the parameters inside the current modal before exiting.
   */
  private resetModal(): void {
    this.name = '';
    this.dictContent = '';
    this.separator = '';
  }

  /**
   * Generates a random dictionary UUID for the new dictionary generated.
   */
  private getDictionaryRandomID(): string {
    return 'dictionary-' + uuid();
  }
}
