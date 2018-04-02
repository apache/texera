import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorLabelComponent } from './operator-label.component';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../../service/operator-metadata/stub-operator-metadata.service';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import {CustomNgMaterialModule} from '../../../../common/custom-ng-material.module';
import { MOCK_OPERATOR_SCHEMA_LIST } from '../../../service/operator-metadata/mock-operator-metadata.data';
import { By } from '@angular/platform-browser';

describe('OperatorLabelComponent', () => {
  const mockOperatorData = MOCK_OPERATOR_SCHEMA_LIST[0];
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OperatorLabelComponent ],
      imports: [ CustomNgMaterialModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorLabelComponent);
    component = fixture.componentInstance;

    // use one mock operator schema as input to construct the operator label
    component.operator = mockOperatorData;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create an ID', () => {
    expect(component.operatorLabelID).toContain('texera-operator-label-');
  });

  it('should display operator user friendly name on the UI', () => {
    const element = <HTMLElement>(fixture.debugElement.query(By.css('.texera-operator-label-body')).nativeElement);
    expect(element.innerHTML.trim()).toEqual(mockOperatorData.additionalMetadata.userFriendlyName);
  });

});
