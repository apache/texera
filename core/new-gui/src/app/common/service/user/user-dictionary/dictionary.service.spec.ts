import { HttpClient } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, flush, inject, TestBed, tick } from '@angular/core/testing';
import { from, NEVER, Subject } from 'rxjs';
import { AppSettings } from 'src/app/common/app-setting';
import { DictionaryService, EVENT_TYPE, NotReadyError, USER_DICT_EVENT, UserDictionary } from './dictionary.service';

describe('DictionaryService', () => {
  let dictionaryService: DictionaryService;
  let testDict: UserDictionary;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        DictionaryService
      ],
      imports: [
        HttpClientTestingModule
      ]
    });

    dictionaryService = TestBed.inject(DictionaryService);
    testDict = {a: 'a', b: 'b', c: 'c'}; // sample dictionary used throughout testing
  });

  it('should be created', inject([DictionaryService], (injectedService: DictionaryService) => {
    expect(injectedService).toBeTruthy();
  }));

  describe('Dictionary I/O', () => {

    describe('Backend interface', () => {
      let httpMock: HttpTestingController;

      beforeEach(() => {
        httpMock = TestBed.inject(HttpTestingController);
        // handle the getAll() request created when initializing dictionaryService
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`).flush({
          code: 1,
          result: testDict
        });
      });

      it('should produce a POST request when get() is called', fakeAsync(() => {
        const testKey = 'test';
        dictionaryService.get(testKey);
        // get() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`);
        // POST request should have a properly formatted json payload
        expect(req.cancelled).toBeFalsy();
        expect(req.request.method).toEqual('POST');
        expect(req.request.responseType).toEqual('json');
        expect(req.request.body).toEqual({requestType: 1, key: testKey});
        req.flush({code: 0, result: 'testValue'});
        flush();
        httpMock.verify();
      }));

      it('should produce a POST request when getAll() is called', fakeAsync(() => {
        dictionaryService.getAll();
        // getAll() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`);
        // POST request should have a properly formatted json payload
        expect(req.cancelled).toBeFalsy();
        expect(req.request.method).toEqual('POST');
        expect(req.request.responseType).toEqual('json');
        expect(req.request.body).toEqual({requestType: 2});
        req.flush({code: 1, result: testDict});
        flush();
        httpMock.verify();
      }));

      it('should produce a POST request when set() is called', fakeAsync(() => {
        const testKey = 'test';
        const testValue = 'testValue';
        dictionaryService.set(testKey, testValue);
        // set() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/set`);
        // POST request should have a properly formatted json payload
        expect(req.cancelled).toBeFalsy();
        expect(req.request.method).toEqual('POST');
        expect(req.request.responseType).toEqual('json');
        expect(req.request.body).toEqual({key: testKey, value: testValue});
        req.flush({code: 2, result: 'arbitrary confirmation message'});
        flush();
        httpMock.verify();
      }));

      it('should produce a POST request when delete() is called', fakeAsync(() => {
        const testKey = 'test';
        dictionaryService.delete(testKey);
        // delete() generates a DELETE request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/delete`);
        // DELETE request should have a properly formatted json payload
        expect(req.cancelled).toBeFalsy();
        expect(req.request.method).toEqual('DELETE');
        expect(req.request.responseType).toEqual('json');
        expect(req.request.body).toEqual({key: testKey});
        req.flush({code: 2, result: 'arbitrary confirmation message'});
        flush();
        httpMock.verify();
      }));
    });

    describe('Dictionary Event Stream', () => {
      let httpMock: HttpTestingController;
      let dictEventSubjectNextSpy: jasmine.Spy;

      beforeEach(() => {
        httpMock = TestBed.inject(HttpTestingController);
        // spy on subject.next() function used to broadcast new dictionary events
        dictEventSubjectNextSpy = spyOn((dictionaryService as any).dictionaryEventSubject, 'next');

        // handle the getAll() request created when initializing dictionaryService
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`).flush({
          code: 1,
          result: testDict
        });
        dictEventSubjectNextSpy.calls.reset();
      });

      it('should emit an event when get() is called', fakeAsync(() => {
        dictionaryService.get('test');
        // get() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`);
        // fulfill request with proper output
        req.flush({code: 0, result: 'testValue'});
        tick();
        // expect event to be emitted
        expect(dictEventSubjectNextSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.GET, key: 'test', value: 'testValue'});
        dictEventSubjectNextSpy.calls.reset();
        flush();
        httpMock.verify();
      }));

      it('should emit an event when getAll() is called', fakeAsync(() => {
        dictionaryService.getAll();
        // getAll() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`);
        // fulfill request with proper output
        req.flush({code: 1, result: testDict});
        tick();
        // expect event to be emitted
        expect(dictEventSubjectNextSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.GET_ALL, value: testDict});
        dictEventSubjectNextSpy.calls.reset();
        flush();
        httpMock.verify();
      }));

      it('should emit an event when set() is called', fakeAsync(() => {
        dictionaryService.set('test', 'testValue');
        // set() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/set`);
        // fulfill request with proper output
        req.flush({code: 2, result: 'arbitrary confirmation message'});
        tick();
        // expect event to be emitted
        expect(dictEventSubjectNextSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.SET, key: 'test', value: 'testValue'});
        dictEventSubjectNextSpy.calls.reset();
        flush();
        httpMock.verify();
      }));

      it('should emit an event when delete() is called', fakeAsync(() => {
        dictionaryService.delete('test');
        // delete() generates a POST request to this url
        const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/delete`);
        // fulfill request with proper output
        req.flush({code: 2, result: 'arbitrary confirmation message'});
        tick();
        // expect event to be emitted
        expect(dictEventSubjectNextSpy).toHaveBeenCalledOnceWith({type: EVENT_TYPE.DELETE, key: 'test'});
        dictEventSubjectNextSpy.calls.reset();
        flush();
        httpMock.verify();
      }));
    });

    describe('Function Return Values', () => {
      let httpMock: HttpTestingController;

      beforeEach(() => {
        httpMock = TestBed.inject(HttpTestingController);
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`).flush({
          code: 1,
          result: testDict
        });
      });

      it('get() should yield string value', fakeAsync(() => {
        const testKey = 'test';
        const testValue = 'testValue';
        // get() should output testValue
        dictionaryService.get(testKey).subscribe(x => expect(x).toEqual(testValue));
        // fulfill request with testValue
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`)
          .flush({code: 0, result: testValue});
        flush();
        httpMock.verify();
      }));

      it('getAll() should yield entire dict as an object w/ attributes as key/value pairs', fakeAsync(() => {
        // getAll() should output testDict
        dictionaryService.getAll().subscribe(x => expect(x).toEqual(testDict));
        // fulfill request with testDict
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`)
          .flush({code: 1, result: testDict});
        flush();
        httpMock.verify();
      }));

      it('set() should yield true after assignment', fakeAsync(() => {
        const testKey = 'test';
        const testValue = 'testValue';
        // set() should output true when successful
        dictionaryService.set(testKey, testValue).subscribe(x => expect(x).toEqual(true));
        // fulfill request to indicate success
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/set`)
          .flush({code: 2, result: 'arbitrary confirmation message'});
        flush();
        httpMock.verify();
      }));

      it('set() should yield true after deletion', fakeAsync(() => {
        const testKey = 'test';
        // delete() should output true when successful
        dictionaryService.delete(testKey).subscribe(x => expect(x).toEqual(true));
        // fulfill request to indicate success
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/delete`)
          .flush({code: 2, result: 'arbitrary confirmation message'});
        flush();
        httpMock.verify();
      }));
    });

  });

  describe('Dictionary Proxy', () => {
    describe('Init', () => {
      it('should throw exceptions if a dictionary is requested before initialization', () => {
        // make sure getAll, and initialization, never finishes
        spyOn(DictionaryService.prototype, 'getAll').and.returnValue(NEVER);
        dictionaryService = new DictionaryService(TestBed.inject(HttpClient));

        // attempt to get user dictionary
        expect(() => dictionaryService.getUserDictionary()).toThrowError(NotReadyError);
      });

      it('should return an empty dictionary if forced to before initialization', () => {
        // make sure getAll, and initialization, never finishes
        spyOn(DictionaryService.prototype, 'getAll').and.returnValue(NEVER);
        dictionaryService = new DictionaryService(TestBed.inject(HttpClient));

        // attempt to get user dictionary
        const dictionary = dictionaryService.forceGetUserDictionary();
        expect((Object.keys(dictionary).length)).toEqual(0);
      });

      it('should only publish dictionary once initialized', fakeAsync(() => {
        let gotUserDictionary = false;
        let completeInitialization: (value: UserDictionary) => void = () => {};

        // make sure getAll, and initialization, only finishes if completeInitialization is called
        spyOn(DictionaryService.prototype, 'getAll').and.returnValue(from(new Promise<UserDictionary>((resolve) => {
          completeInitialization = resolve;
        })));
        dictionaryService = new DictionaryService(TestBed.inject(HttpClient));

        // setup subscription so that gotUserDictionary is only true when observable outputs the UserDictionary
        const dictionaryPromise = dictionaryService.getUserDictionaryAsync();
        dictionaryPromise.subscribe(() => gotUserDictionary = true);
        tick();
        expect(gotUserDictionary).toBeFalse();
        completeInitialization(testDict);
        tick();
        expect(gotUserDictionary).toBeTrue();
      }));

      it('should initialize the local dictionary properly', fakeAsync(() => {
        const httpMock = TestBed.inject(HttpTestingController);
        // handle getAll() from init of dictionaryService in beforeEach. we won't be using that
        httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`).flush({
          code: 1,
          result: testDict
        });

        // reinject DictionaryService to use fakeAsync context (beforeEach wasn't in a fakeAsync zone)
        dictionaryService = new DictionaryService(TestBed.inject(HttpClient));

        // complete init by fulfilling POST request generated by initial getAll()
        const mockReq = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`);
        mockReq.flush({code: 1, result: testDict});
        tick();

        // expect all dictionary getters to confirm that all proxy dicts match testDict
        let dict = dictionaryService.getUserDictionary();
        expect(dict).toEqual(testDict);
        dict = dictionaryService.forceGetUserDictionary();
        expect(dict).toEqual(testDict);
        dictionaryService.getUserDictionaryAsync().subscribe(value => expect(value).toEqual(testDict));
        tick();
      }));
    });

    it('Dictionary events should update the local dictionary', fakeAsync(() => {
      const dict = {a: 'a', b: 'b', c: 'c'};
      const localDict = (dictionaryService as any).localUserDictionary;
      const subject: Subject<USER_DICT_EVENT> = (dictionaryService as any).dictionaryEventSubject;

      subject.next({type: EVENT_TYPE.GET_ALL, value: dict});
      tick();
      expect(localDict).toEqual(dict);

      subject.next({type: EVENT_TYPE.GET, key: 'testKey', value: 'testValue'});
      tick();
      expect(localDict.testKey).toEqual('testValue');

      subject.next({type: EVENT_TYPE.SET, key: 'testKey2', value: 'testValue2'});
      tick();
      expect(localDict.testKey2).toEqual('testValue2');

      subject.next({type: EVENT_TYPE.DELETE, key: 'testKey2'});
      tick();
      expect(localDict.testKey2).toBeUndefined();
    }));

    it('Proxy dicts should stay in sync with localdict after dictionary events', fakeAsync(() => {
      const dict = {a: 'a', b: 'b', c: 'c'};
      const localDict = (dictionaryService as any).localUserDictionary; // base dictionary referenced by proxy dictionaries
      const proxyDict = dictionaryService.forceGetUserDictionary(); // proxy dict
      const subject: Subject<USER_DICT_EVENT> = (dictionaryService as any).dictionaryEventSubject;

      subject.next({type: EVENT_TYPE.GET_ALL, value: dict});
      tick();
      expect(localDict).toEqual(dict);
      expect(proxyDict).toEqual(localDict);

      subject.next({type: EVENT_TYPE.GET, key: 'testKey', value: 'testValue'});
      tick();
      expect(localDict.testKey).toEqual('testValue');
      expect(proxyDict).toEqual(localDict);

      subject.next({type: EVENT_TYPE.SET, key: 'testKey2', value: 'testValue2'});
      tick();
      expect(localDict.testKey2).toEqual('testValue2');
      expect(proxyDict).toEqual(localDict);

      subject.next({type: EVENT_TYPE.DELETE, key: 'testKey2'});
      tick();
      expect(localDict.testKey2).toBeUndefined();
      expect(proxyDict).toEqual(localDict);
    }));
  });

});
