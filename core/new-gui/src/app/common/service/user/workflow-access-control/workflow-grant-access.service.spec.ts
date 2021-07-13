import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import {ReactiveFormsModule} from "@angular/forms";
import { AppSettings } from '../../../app-setting';
import { WorkflowGrantAccessService } from './workflow-grant-access.service';

describe('WorkflowPersistService', () => {
  let service: WorkflowGrantAccessService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule
      ]
    });
    service = TestBed.inject(WorkflowGrantAccessService);

  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
