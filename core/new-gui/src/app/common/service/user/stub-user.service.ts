// import { Injectable } from '@angular/core';
// import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';

// import { Observable } from 'rxjs/Observable';
// import { environment } from '../../../../environments/environment';
// import { EventEmitter } from '@angular/core';
// import { observable, Subject } from 'rxjs';
// import { User } from '../../type/user';
// import { UserWebResponse } from '../../type/user';
// import { UserService } from './user.service';

// export const MOCK_USER_ID = 1;
// export const MOCK_USER_NAME = 'testUser';
// export const MOCK_USER = {
//   userName: MOCK_USER_NAME,
//   userID: MOCK_USER_ID
// };

// type PublicInterfaceOf<Class> = {
//   [Member in keyof Class]: Class[Member];
// };

// /**
//  * This StubUserService is to test other service's functionality that depends on UserService
//  * The login/register will succeed when receive the user name {@link stubUserName} and fail otherwise.
//  * It will correctly emit UserChangedEvent as the normal UserService do.
//  */
// @Injectable()
// export class StubUserService implements PublicInterfaceOf<UserService> {
//   public userChangedEvent: Subject<User | undefined> = new Subject();
//   public user: User | undefined;

//   constructor() {
//     this.user = MOCK_USER;
//     this.userChangedEvent.next(this.user);
//   }

//   public getUser(): User | undefined {
//     return this.user;
//   }

//   public register(userName: string): Observable<UserWebResponse> {
//     throw new Error('unimplemented');
//   }

//   public login(userName: string): Observable<UserWebResponse> {
//     if (this.user) {
//       throw new Error('user is already logged in');
//     }
//     throw new Error('unimplemented');
//   }

//   public logOut(): void {
//     throw new Error('unimplemented');
//   }

//   public isLogin(): boolean {
//     return this.user !== undefined;
//   }

//   public getUserChangedEvent(): Observable<User | undefined> {
//     return this.userChangedEvent.asObservable();
//   }

//   private changeUser(user: User | undefined): void {
//     this.user = user;
//   }

// }
