import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorPanelComponent } from './operator-panel.component';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { GroupInfo, OperatorSchema } from '../../types/operator-schema';

import {
  MOCK_OPERATOR_METADATA, MOCK_OPERATOR_GROUPS,
  MOCK_OPERATOR_SCHEMA_LIST
} from '../../service/operator-metadata/mock-operator-metadata.data';

import * as c from './operator-panel.component';
import '../../../common/rxjs-operators';
import { By } from '@angular/platform-browser';

describe('OperatorPanelComponent', () => {
  let component: OperatorPanelComponent;
  let fixture: ComponentFixture<OperatorPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorPanelComponent],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('getGroupNamesSorted test 1', () => {
    const groups = MOCK_OPERATOR_GROUPS;

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['Source', 'Analysis', 'View Results']);

  });

  it('getGroupNamesSorted test 2', () => {
    const groups: GroupInfo[] = [
      { groupName: 'group_1', groupOrder: 1 },
      { groupName: 'group_2', groupOrder: 100 }
    ];

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['group_1', 'group_2']);

  });

  it('getOperatorGroupMap test 1', () => {
    const opMetadata = MOCK_OPERATOR_METADATA;

    const result = c.getOperatorGroupMap(opMetadata);

    const expectedResult = new Map<string, OperatorSchema[]>();
    expectedResult.set('Source', [opMetadata.operators[0]]);
    expectedResult.set('Analysis', [opMetadata.operators[1]]);
    expectedResult.set('View Results', [opMetadata.operators[2]]);

    expect(result).toEqual(expectedResult);

  });

  it('should receive operator metadata from service', () => {
    // if the length of our schema list is equal to the length of mock data
    // we assume the mock data has been received
    expect(component.operatorSchemaList.length).toEqual(MOCK_OPERATOR_SCHEMA_LIST.length);
    expect(component.groupNamesOrdered.length).toEqual(MOCK_OPERATOR_GROUPS.length);
  });

  it('should have all operator names shown in the UI side panel', () => {
    // get all the group elements, then map to their inner HTML text
    const operatorNamesInUI = fixture.debugElement
      .queryAll(By.css('.texera-operator-group-name'))
      .map(el => <HTMLElement>el.nativeElement)
      .map(el => el.innerHTML.trim());

    // check the UI text is the same with mock data
    expect(operatorNamesInUI).toEqual(
      MOCK_OPERATOR_GROUPS.map(groupInfo => groupInfo.groupName));

  });

  it('should make operator label visible when clicking a group name', () => {
    // get one of the operator group name
    const firstGroupPanelDebugElement = fixture.debugElement.query(By.css('.texera-operator-group-panel'));

    const firstOperatorLabelDebugElement = firstGroupPanelDebugElement.query(By.css('.texerea-operator-name-wrapper'));

    // console.log(firstOperatorLabelDebugElement);
    // console.log(());
    const styles = window.getComputedStyle(<HTMLElement>firstOperatorLabelDebugElement.nativeElement, null);
    console.log(styles['visibility']);

    // trigger a click on this group name
    firstGroupPanelDebugElement.triggerEventHandler('click', null);

    console.log(styles['visibility']);

    firstGroupPanelDebugElement.triggerEventHandler('click', null);

    console.log(styles['visibility']);



  });

});
