import { Component } from "@angular/core";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { from } from "rxjs";
import { cloneDeep } from "lodash-es";

import { NgbdModalResourceAddComponent } from "./ngbd-modal-resource-add/ngbd-modal-resource-add.component";
import { NgbdModalResourceDeleteComponent } from "./ngbd-modal-resource-delete/ngbd-modal-resource-delete.component";
import { NgbdModalResourceViewComponent } from "./ngbd-modal-resource-view/ngbd-modal-resource-view.component";
import { UserDictionary } from "../../../../common/type/user-dictionary";
import { UserDictionaryService } from "../../../service/user-dictionary/user-dictionary.service";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

const DICTIONARY_ITEM_PREVIEW_SIZE = 20;

/**
 * UserDictionarySectionComponent is the main interface
 * for managing all the user dictionaries. On this interface,
 * user can view all the dictionaries by the order he/she defines,
 * upload dictionary, and delete dictionary.
 *
 * @author Zhaomin Li
 */
@UntilDestroy()
@Component({
  selector: "texera-user-dictionary-section",
  templateUrl: "./user-dictionary-section.component.html",
  styleUrls: ["./user-dictionary-section.component.scss", "../../dashboard.component.scss"],
})
export class UserDictionarySectionComponent {
  constructor(
    private userDictionaryService: UserDictionaryService,
    private userService: UserService,
    private modalService: NgbModal
  ) {
    this.userDictionaryService.refreshDictionaries();
    this.forceRefreshMatChip();
  }

  /**
   * openNgbdModalResourceViewComponent triggers the view dictionary
   * component. It calls the method in service to send request to
   * backend and to fetch info package for a specific dictionary.
   * addModelObservable receives information about adding a item
   * into dictionary and calls method in service. deleteModelObservable
   * receives information about deleting a item in dictionary and
   * calls method in service.
   *
   * @param dictionary: the dictionary that user wants to view
   */
  public openNgbdModalResourceViewComponent(dictionary: UserDictionary): void {
    const modalRef = this.modalService.open(NgbdModalResourceViewComponent);
    modalRef.componentInstance.dictionary = dictionary;
  }

  /**
   * openNgbdModalResourceAddComponent triggers the add dictionary
   * component. The component returns the information of new dictionary,
   *  and this method adds new dictionary in to the list on UI. It calls
   * the addUserDictionaryData method in to store user-define dictionary,
   * or uploadDictionary in service to upload dictionary file.
   *
   *
   * @param
   */
  public openNgbdModalResourceAddComponent(): void {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);
  }

  /**
   * openNgbdModalResourceDeleteComponent trigger the delete
   * dictionary component. If user confirms the deletion, the method
   * sends message to frontend and delete the dicrionary on backend and
   * update the frontend. It calls the deleteUserDictionaryData method
   * in service which using backend API.
   *
   * @param dictionary: the dictionary that user wants to remove
   */
  public openNgbdModalResourceDeleteComponent(dictionary: UserDictionary): void {
    const modalRef = this.modalService.open(NgbdModalResourceDeleteComponent);
    modalRef.componentInstance.dictionary = cloneDeep(dictionary);

    from(modalRef.result)
      .pipe(untilDestroyed(this))
      .subscribe((confirmDelete: boolean) => {
        if (confirmDelete) {
          this.userDictionaryService.deleteDictionary(dictionary.id);
        }
      });
  }

  /**
   * TODO: the sorting function below is disabled due to the huge structural change in the dashboard
   * These methods haven't been changed after that, and thus they won't work.
   * The buttons are kept for future recovery.
   */

  /**
   * sort the dictionary by name in ascending order
   *
   * @param
   */
  public ascSort(): void {
    // this.userDictionaries.sort((t1, t2) => {
    //   if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return 1; }
    //   if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return -1; }
    //   return 0;
    // });
  }

  /**
   * sort the dictionary by name in descending order
   *
   * @param
   */
  public dscSort(): void {
    // this.userDictionaries.sort((t1, t2) => {
    //   if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return -1; }
    //   if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return 1; }
    //   return 0;
    // });
  }

  /**
   * sort the dictionary by size
   *
   * @param
   */
  public sizeSort(): void {
    // this.userDictionaries.sort((t1, t2) => {
    //   if (t1.items.length > t2.items.length) { return -1; }
    //   if (t1.items.length < t2.items.length) { return 1; }
    //   return 0;
    // });
  }

  public disableAddbutton(): boolean {
    return !this.userService.isLogin();
  }

  public getDictArray(): ReadonlyArray<UserDictionary> {
    const dictionaryArray = this.userDictionaryService.getUserDictionaries();
    if (!dictionaryArray) {
      return [];
    }
    return dictionaryArray;
  }

  public limitPreviewItemSize(item: string): string {
    return item.length <= DICTIONARY_ITEM_PREVIEW_SIZE ? item : item.substr(0, DICTIONARY_ITEM_PREVIEW_SIZE) + "...";
  }

  /**
   * Temporary solution to force the mat chip to refresh
   * Unknown problem happens on mat chip after upgrading to Angular 9
   * open any ngbd modal and close it will solve the problem
   */
  private forceRefreshMatChip(): void {
    const modalRef = this.modalService.open(NgbdModalResourceViewComponent);
    modalRef.close();
  }
}
