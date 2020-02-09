import { TestBed } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { OperatorMetadataService, EMPTY_OPERATOR_METADATA } from './operator-metadata.service';

import '../../../common/rxjs-operators';
import { mockOperatorMetaData } from './mock-operator-metadata.data';


describe('OperatorMetadataService', () => {

  let service: OperatorMetadataService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
      ],
      providers: [
        OperatorMetadataService,
        HttpClient
      ]
    });

    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    service = TestBed.get(OperatorMetadataService);
  });


  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should emit an empty operator metadata first', () => {
    service.getOperatorMetadata().first().subscribe(
      value => expect(value).toEqual(EMPTY_OPERATOR_METADATA)
    );
  });

  it('should send http request once', () => {
    service.getOperatorMetadata().last().subscribe(
      value => expect(value).toBeTruthy()
    );
    httpTestingController.expectOne(
      request => request.method === 'GET'
    );
  });

  it('should check if operatorType exists correctly', () => {
    service.getOperatorMetadata().last().subscribe(
      () => {
        expect(service.operatorTypeExists('ScanSource')).toBeTruthy();
        expect(service.operatorTypeExists('InvalidOperatorType')).toBeFalsy();
      }
    );
    const req = httpTestingController.match(
      request => request.method === 'GET'
    );
    req[0].flush(mockOperatorMetaData);
  });

});
