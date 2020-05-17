import { Injectable, EventEmitter } from '@angular/core';
import { UserFileService } from './user-file.service';
import { HttpClient } from '@angular/common/http';
import { UserAccountService } from '../user-account/user-account.service';


@Injectable()
export class  StubUserFileService extends UserFileService {

  constructor(
    private stubUserAccountService: UserAccountService,
    private stubHttp: HttpClient
    ) {
    super(stubUserAccountService, stubHttp);
  }

  /**
   * only needs a function here to get called in order to test other related service
   */
  public refreshFiles(): void {
    return;
  }
}
