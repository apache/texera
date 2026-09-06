/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import {
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges,
} from "@angular/core";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { ResourceRegistryService } from "../../../service/user/resource-registry/resource-registry.service";
import { EntityType } from "../../../../hub/service/hub.service";
import { OwnerScope } from "../../../type/owner-scope";
import { forkJoin, Observable, of, Subject } from "rxjs";
import { catchError, switchMap } from "rxjs/operators";
import { SearchFilterParameters } from "../../../type/search-filter-parameters";
import { UserService } from "../../../../common/service/user/user.service";
import { NzDropdownADirective, NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzSpaceCompactItemDirective, NzSpaceCompactComponent } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzDatePickerComponent, NzRangePickerComponent } from "ng-zorro-antd/date-picker";
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { NzMenuDirective, NzMenuItemComponent, NzSubMenuComponent } from "ng-zorro-antd/menu";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";

@UntilDestroy()
@Component({
  selector: "texera-filters",
  templateUrl: "./filters.component.html",
  styleUrls: ["./filters.component.scss"],
  imports: [
    NzDropdownADirective,
    NzDropdownDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDropdownMenuComponent,
    NzDatePickerComponent,
    NzRangePickerComponent,
    FormsModule,
    NgFor,
    NzMenuDirective,
    NzMenuItemComponent,
    NzCheckboxComponent,
    NgIf,
    NzSubMenuComponent,
    NzSpaceCompactComponent,
  ],
})
export class FiltersComponent implements OnInit, OnChanges {
  public isLogin = this.userService.isLogin();
  /** Fires when the kind or scope changes, to refetch the owner facet. */
  private readonly facetReload$ = new Subject<void>();
  private _masterFilterList: ReadonlyArray<string> = [];
  /** Which kind this page lists; decides whose owners and ids are offered. `null` spans every kind. */
  @Input() public entityType: EntityType | null = EntityType.Workflow;
  /** Which people the Owner facet offers, matching the results the host page shows. */
  @Input() public ownerScope: OwnerScope = "accessible";
  @Output()
  public masterFilterListChange = new EventEmitter<typeof this._masterFilterList>();
  public get masterFilterList(): ReadonlyArray<string> {
    return this._masterFilterList;
  }
  public set masterFilterList(value: ReadonlyArray<string>) {
    this.setMasterFilterList(value, true);
  }
  private setMasterFilterList(value: ReadonlyArray<string>, updateDropdown: boolean) {
    if (
      !this._masterFilterList ||
      !value ||
      this._masterFilterList.length !== value.length ||
      this._masterFilterList.some((v, i) => v !== value[i])
    ) {
      // Only update when there is a change to prevent unnecessary calls to search.
      this._masterFilterList = value;
      if (updateDropdown) {
        this.updateDropdownMenus(value);
      }
      this.masterFilterListChange.emit(value);
    }
  }
  public owners: { userName: string; checked: boolean }[] = [];
  public wids: { id: string; checked: boolean }[] = [];
  public operatorGroups: string[] = [];
  public operators: Map<
    string,
    { userFriendlyName: string; operatorType: string; operatorGroup: string; checked: boolean }[]
  > = new Map();
  public selectedCtime: Date[] = [];
  public selectedMtime: Date[] = [];
  public selectedOwners: string[] = [];
  public selectedIDs: string[] = [];
  public selectedOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
  public searchCriteria: string[] = ["owner", "id", "ctime", "mtime", "operator"];

  constructor(
    private userService: UserService,
    private operatorMetadataService: OperatorMetadataService,
    private notificationService: NotificationService,
    private resourceRegistry: ResourceRegistryService,
    private cdr: ChangeDetectorRef
  ) {}

  /**
   * The id dropdown is hidden for kinds with no id-listing endpoint. A page listing every kind keeps
   * the workflow ids it had before: that page lists workflows too, and the backend binds `id=` to
   * the workflow arm, exactly as the operator facet is workflow-only.
   */
  public get hasIdFilter(): boolean {
    return this.resourceRegistry.get(this.entityType ?? EntityType.Workflow).retrieveIds !== undefined;
  }

  ngOnInit(): void {
    this.trackLoginState();
    this.searchParameterBackendSetup();
    this.facetReload$
      .pipe(
        // switchMap: without it a stale response can land last and refill the facet.
        switchMap(() =>
          forkJoin({
            owners: this.ownersForCurrentScope(),
            ids: this.idsForCurrentKind(),
          })
        ),
        untilDestroyed(this)
      )
      .subscribe(facets => this.applyFacets(facets));
    // Through the subject, not a separate subscribe: an init response that landed after a tab
    // switch would otherwise overwrite the new kind's facets.
    this.facetReload$.next();
  }

  ngOnChanges(changes: SimpleChanges): void {
    const changed = changes["entityType"] ?? changes["ownerScope"];
    // ngOnChanges runs before ngOnInit, so the first pass is left to ngOnInit's single load.
    if (changed && !changed.firstChange) {
      this.reloadFacets();
    }
  }

  /** Refetches both facets for the kind and scope now in effect. */
  private reloadFacets(): void {
    this.owners = [];
    this.wids = [];
    this.facetReload$.next();
  }

  /** The owners this page should offer; none for a signed-out visitor, whose endpoints are gated. */
  private ownersForCurrentScope(): Observable<string[]> {
    return this.isLogin ? this.resourceRegistry.ownersFor(this.entityType, this.ownerScope) : of([]);
  }

  /** The ids this kind offers, or none for a kind with no id endpoint. */
  private idsForCurrentKind(): Observable<number[]> {
    if (!this.isLogin) {
      return of([]);
    }
    // Same rule as the owner leg: a failed request costs its own facet, not the subscription, which
    // would otherwise end here and leave every later tab switch blanking both dropdowns.
    const descriptor = this.resourceRegistry.get(this.entityType ?? EntityType.Workflow);
    return (descriptor.retrieveIds?.() ?? of([])).pipe(catchError(() => of([])));
  }

  /** Re-runs the tag list: a surviving selection stays checked, one that does not is dropped and reported. */
  private applyFacets({ owners, ids }: { owners: string[]; ids: number[] }): void {
    this.owners = owners.map(name => ({ userName: name, checked: false }));
    this.wids = ids.map(id => ({ id: id.toString(), checked: false }));
    this.updateDropdownMenus(this.masterFilterList);
  }

  private trackLoginState(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        if (this.isLogin) {
          // Signing in mid-page used to leave the facet empty until a reload.
          this.reloadFacets();
        } else {
          // Signing out: clear rather than refetch, or an expired session fires the authenticated
          // endpoints anyway and the empty result drops the chips with an "Invalid owner name" toast.
          this.owners = [];
          this.wids = [];
        }
        this.cdr.detectChanges();
      });
  }

  /** Backend calls for the filtered kind's owners and ids, plus the operator metadata. */
  private searchParameterBackendSetup() {
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(opdata => {
        opdata.groups.forEach(group => {
          this.operators.set(
            group.groupName,
            opdata.operators
              .filter(operator => operator.additionalMetadata.operatorGroupName === group.groupName)
              .map(operator => {
                return {
                  userFriendlyName: operator.additionalMetadata.userFriendlyName,
                  operatorType: operator.operatorType,
                  operatorGroup: operator.additionalMetadata.operatorGroupName,
                  checked: false,
                };
              })
          );
        });
        this.operatorGroups = opdata.groups.map(group => group.groupName);
      });
  }

  /**
   * updates selectedOwners array to match owners checked in dropdown menu
   */
  public updateSelectedOwners(): void {
    this.selectedOwners = this.owners.filter(owner => owner.checked).map(owner => owner.userName);
    this.buildMasterFilterList();
  }

  /**
   * updates selectedIDs array to match worfklow ids checked in dropdown menu
   */
  public updateSelectedIDs(): void {
    this.selectedIDs = this.wids.filter(wid => wid.checked).map(wid => wid.id);
    this.buildMasterFilterList();
  }

  /**
   * updates selectedOperators array to match operators checked in dropdown menu
   */
  public updateSelectedOperators(): void {
    const filteredOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
    Array.from(this.operators.values())
      .flat()
      .forEach(operator => {
        if (operator.checked) {
          filteredOperators.push({
            userFriendlyName: operator.userFriendlyName,
            operatorType: operator.operatorType,
            operatorGroup: operator.operatorGroup,
          });
        }
      });
    this.selectedOperators = filteredOperators;
    this.buildMasterFilterList();
  }

  /**
   * updates dropdown menus when nz-select bar is changed
   */
  public updateDropdownMenus(tagListString: ReadonlyArray<string>): void {
    //operators array is not cleared, so that operator object properties can be used for reconstruction of the array
    //operators map is too expensive/difficult to search for operator object properties
    this.selectedIDs = [];
    this.selectedOwners = [];
    let newSelectedOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
    this.selectedCtime = [];
    this.selectedMtime = [];
    this.setDropdownSelectionsToUnchecked();
    tagListString.forEach(tag => {
      if (tag.includes(":")) {
        const searchArray = tag.split(":");
        const searchField = searchArray[0];
        const searchValue = searchArray[1].trim();
        const date_regex =
          /^(\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01]) ~ (\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01])$/;
        const searchDate: RegExpMatchArray | null = searchValue.match(date_regex);
        switch (searchField) {
          case "owner":
            const selectedOwnerIndex = this.owners.findIndex(owner => owner.userName === searchValue);
            if (selectedOwnerIndex === -1) {
              this.removeInvalidFilterTag(tag);
              this.notificationService.error("Invalid owner name");
              break;
            }
            this.owners[selectedOwnerIndex].checked = true;
            this.selectedOwners.push(searchValue);
            break;
          case "id":
            const selectedIDIndex = this.wids.findIndex(wid => wid.id === searchValue);
            if (selectedIDIndex === -1) {
              this.removeInvalidFilterTag(tag);
              this.notificationService.error("Invalid workflow id");
              break;
            }
            this.wids[selectedIDIndex].checked = true;
            this.selectedIDs.push(searchValue);
            break;
          case "operator":
            const selectedOperator = this.selectedOperators.find(operator => operator.userFriendlyName === searchValue);
            if (!selectedOperator) {
              this.removeInvalidFilterTag(tag);
              this.notificationService.error("Invalid operator name");
              break;
            }
            newSelectedOperators.push(selectedOperator);
            const operatorSublist = this.operators.get(selectedOperator.operatorGroup);
            if (operatorSublist) {
              for (let operator of operatorSublist) {
                if (operator.userFriendlyName === searchValue) {
                  operator.checked = true;
                  break;
                }
              }
            }
            break;
          case "ctime": //should only run at most once
            if (this.selectedCtime.length > 0) {
              // if there is already an selected date, ignore the subsequent ctime tags
              this.notificationService.error("Multiple search dates is not allowed");
              break;
            }
            if (!searchDate) {
              this.notificationService.error("Date format is incorrect");
              break;
            }
            this.selectedCtime[0] = new Date(
              parseInt(searchDate[1]),
              parseInt(searchDate[2]) - 1,
              parseInt(searchDate[3])
            );
            this.selectedCtime[1] = new Date(
              parseInt(searchDate[4]),
              parseInt(searchDate[5]) - 1,
              parseInt(searchDate[6])
            );
            break;
          case "mtime": //should only run at most once
            if (this.selectedMtime.length > 0) {
              // if there is already an selected date, ignore the subsequent ctime tags
              this.notificationService.error("Multiple search dates is not allowed");
              break;
            }
            if (!searchDate) {
              this.notificationService.error("Date format is incorrect");
              break;
            }
            this.selectedMtime[0] = new Date(
              parseInt(searchDate[1]),
              parseInt(searchDate[2]) - 1,
              parseInt(searchDate[3])
            );
            this.selectedMtime[1] = new Date(
              parseInt(searchDate[4]),
              parseInt(searchDate[5]) - 1,
              parseInt(searchDate[6])
            );
            break;
        }
      }
    });
    this.selectedOperators = newSelectedOperators;
    this.buildMasterFilterList();
  }

  private removeInvalidFilterTag(tag: string): void {
    this.setMasterFilterList(
      this.masterFilterList.filter(filterTag => filterTag !== tag),
      false
    );
  }

  /**
   * sets all dropdown menu options to unchecked
   */
  private setDropdownSelectionsToUnchecked(): void {
    this.owners.forEach(owner => {
      owner.checked = false;
    });
    this.wids.forEach(wid => {
      wid.checked = false;
    });
    for (let operatorList of this.operators.values()) {
      operatorList.forEach(operator => (operator.checked = false));
    }
  }

  /**
   * checks if a tag string is a workflow name or dropdown menu search parameter
   */
  private checkIfWorkflowName(tag: string) {
    const stringChecked: string[] = tag.split(":");
    return !(stringChecked.length === 2 && this.searchCriteria.includes(stringChecked[0]));
  }

  /**
   * builds the tags to be displayd in the nz-select search bar
   * - Workflow names with ":" are not allowed due to conflict with other search parameters' format
   */
  public buildMasterFilterList(): void {
    let newFilterList: string[] = this.masterFilterList.filter(tag => this.checkIfWorkflowName(tag));
    newFilterList = newFilterList.concat(this.selectedOwners.map(owner => "owner: " + owner));
    newFilterList = newFilterList.concat(this.selectedIDs.map(id => "id: " + id));
    newFilterList = newFilterList.concat(
      this.selectedOperators.map(operator => "operator: " + operator.userFriendlyName)
    );
    if (this.selectedCtime.length != 0) {
      newFilterList.push(
        "ctime: " +
          this.getFormattedDateString(this.selectedCtime[0]) +
          " ~ " +
          this.getFormattedDateString(this.selectedCtime[1])
      );
    }
    if (this.selectedMtime.length != 0) {
      newFilterList.push(
        "mtime: " +
          this.getFormattedDateString(this.selectedMtime[0]) +
          " ~ " +
          this.getFormattedDateString(this.selectedMtime[1])
      );
    }
    this.setMasterFilterList(this.updateMasterFilterList(this.masterFilterList, newFilterList), false);
  }

  private updateMasterFilterList(masterFilterList: ReadonlyArray<string>, items: string[]): string[] {
    const list = [...masterFilterList];
    // The purpose of this function is to preserve order.
    // Add the item if it doesn't exist.
    for (const item of items) {
      const ctime = item.startsWith("ctime: ");
      const mtime = item.startsWith("mtime: ");
      if (ctime || mtime) {
        const index = list.findIndex(i => i.startsWith(ctime ? "ctime: " : "mtime: "));
        if (index !== -1) {
          list[index] = item;
        } else {
          list.push(item);
        }
      } else {
        const index = list.indexOf(item);
        if (index === -1) {
          list.push(item);
        }
      }
    }
    // Remove ones that doesn't exist in the new list.
    return list.filter(i => items.indexOf(i) !== -1);
  }

  /**
   * returns a formatted string representing a Date object
   */
  private getFormattedDateString(date: Date): string {
    let dateMonth: number = date.getMonth() + 1;
    let dateDay: number = date.getDate();
    return `${date.getFullYear()}-${(dateMonth < 10 ? "0" : "") + dateMonth}-${(dateDay < 10 ? "0" : "") + dateDay}`;
  }

  public getSearchFilterParameters(): SearchFilterParameters {
    return {
      createDateStart: this.selectedCtime.length > 0 ? this.selectedCtime[0] : null,
      createDateEnd: this.selectedCtime.length > 0 ? this.selectedCtime[1] : null,
      modifiedDateStart: this.selectedMtime.length > 0 ? this.selectedMtime[0] : null,
      modifiedDateEnd: this.selectedMtime.length > 0 ? this.selectedMtime[1] : null,
      owners: this.selectedOwners,
      ids: this.selectedIDs,
      operators: this.selectedOperators.map(o => o.operatorType),
    };
  }

  public getSearchKeywords(): string[] {
    return this.masterFilterList.filter(tag => this.checkIfWorkflowName(tag));
  }
}
