import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorPanelComponent } from './operator-panel.component';
import { OperatorMetadataService, EMPTY_OPERATOR_METADATA } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { GroupInfo, OperatorSchema } from '../../types/operator-schema';

import { OPERATOR_METADATA, OPERATOR_GROUPS, OPERATOR_SCHEMA_LIST } from '../../service/operator-metadata/mock-operator-metadata.data';

import * as c from './operator-panel.component';
import '../../../common/rxjs-operators';

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
    const groups = OPERATOR_GROUPS;

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['Source', 'Analysis', 'View Results']);

  });

  it('getGroupNamesSorted test 2', () => {
    const groups: GroupInfo[] = [
      { groupName: 'group_1', groupOrder: 1},
      { groupName: 'group_2', groupOrder: 100}
    ];

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['group_1', 'group_2']);

  });

  it('getGroupNamesSorted test 3 empty list', () => {
    const groups: GroupInfo[] = [];

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual([]);

  });

  it('getOperatorGroupMap test 1', () => {
    const opMetadata = OPERATOR_METADATA;

    const result = c.getOperatorGroupMap(opMetadata);

    const expectedResult = new Map<string, OperatorSchema[]>();
    expectedResult.set('Source', [ opMetadata.operators[0] ]);
    expectedResult.set('Analysis', [ opMetadata.operators[1] ]);
    expectedResult.set('View Results', [ opMetadata.operators[2] ]);

    expect(result).toEqual(expectedResult);

  });

  it('getOperatorGroupMap test 2 - empty operator metadata', () => {
    const result = c.getOperatorGroupMap(EMPTY_OPERATOR_METADATA);

    const expectedResult = new Map<string, OperatorSchema[]>();

    expect(result).toEqual(expectedResult);

  });


});
