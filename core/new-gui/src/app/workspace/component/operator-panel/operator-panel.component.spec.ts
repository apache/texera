import { DragDropService } from './../../service/drag-drop/drag-drop.service';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import '../../../common/rxjs-operators';
import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { OperatorPanelComponent } from './operator-panel.component';
import { OperatorLabelComponent } from './operator-label/operator-label.component';
import { OperatorMetadataService, EMPTY_OPERATOR_METADATA } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { GroupInfo, OperatorSchema } from '../../types/operator-schema.interface';
import { RouterTestingModule } from '@angular/router/testing';
import { TourNgBootstrapModule } from 'ngx-tour-ng-bootstrap';

import {
  mockOperatorMetaData, mockOperatorGroup, mockOperatorSchemaList
} from '../../service/operator-metadata/mock-operator-metadata.data';

import * as c from './operator-panel.component';

class StubDragDropService {

  public registerOperatorLabelDrag(input: any) {}

}


describe('OperatorPanelComponent', () => {
  let component: OperatorPanelComponent;
  let fixture: ComponentFixture<OperatorPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorPanelComponent, OperatorLabelComponent],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: DragDropService, useClass: StubDragDropService},
        TourService
      ],
      imports: [CustomNgMaterialModule, BrowserAnimationsModule, RouterTestingModule.withRoutes([]), TourNgBootstrapModule.forRoot()]
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

  it('should sort group names correctly based on order', () => {
    const groups = mockOperatorGroup;

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['Source', 'Analysis', 'View Results']);

  });

  it('should sort group names correctly based on order relatively. ex: (100, 1) -> (1, 100)', () => {
    const groups: GroupInfo[] = [
      { groupName: 'group_1', groupOrder: 1 },
      { groupName: 'group_2', groupOrder: 100 }
    ];

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['group_1', 'group_2']);

  });

  it('should sort group names correctly from an empty list', () => {
    const groups: GroupInfo[] = [];
    const result = c.getGroupNamesSorted(groups);
    expect(result).toEqual([]);

  });

  it('should generate a map from operator groups to a list operators correctly', () => {
    const opMetadata = mockOperatorMetaData;

    const result = c.getOperatorGroupMap(opMetadata);

    const expectedResult = new Map<string, OperatorSchema[]>();
    expectedResult.set('Source', [opMetadata.operators[0]]);
    expectedResult.set('Analysis', [opMetadata.operators[1]]);
    expectedResult.set('View Results', [opMetadata.operators[2]]);

    expect(result).toEqual(expectedResult);

  });

  it('should generate a map from operator groups to a list operators correctly from an empty list', () => {
    const opMetadata = EMPTY_OPERATOR_METADATA;
    const result = c.getOperatorGroupMap(opMetadata);
    const expectedResult = new Map<string, OperatorSchema[]>();

    expect(result).toEqual(expectedResult);

  });

  it('should receive operator metadata from service', () => {
    // if the length of our schema list is equal to the length of mock data
    // we assume the mock data has been received
    expect(component.operatorSchemaList.length).toEqual(mockOperatorSchemaList.length);
    expect(component.groupNamesOrdered.length).toEqual(mockOperatorGroup.length);
  });

  it('should have all group names shown in the UI side panel', () => {
    const groupNamesInUI = fixture.debugElement
      .queryAll(By.css('.texera-operator-group-name'))
      .map(el => <HTMLElement>el.nativeElement)
      .map(el => el.innerText.trim());

    expect(groupNamesInUI).toEqual(
      mockOperatorGroup.map(group => group.groupName));
  });

  it('should create child operator label component for all operators', () => {
    const operatorLabels = fixture.debugElement
      .queryAll(By.directive(OperatorLabelComponent))
      .map(debugEl => <OperatorLabelComponent>debugEl.componentInstance)
      .map(operatorLabel => operatorLabel.operator);

    expect(operatorLabels.length).toEqual(mockOperatorMetaData.operators.length);
  });

});

