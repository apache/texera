/**
 * This interface stores the information about the user account.
 * These information is used to identify users and to save their data
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserResource.java`
 */
export interface User extends Readonly<{
  userName: string;
  userID: number;
}> { }


export interface UserWebResponseSuccess extends Readonly<{
  code: 0; // 0 represents success and 1 represents error
  user: User;
}> { }

export interface UserWebResponseFailure extends Readonly<{
  code: 1; // 0 represents success and 1 represents error
  message: string;
}> { }

/**
 * This interface is used for communication between frontend and background
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserResource.java`
 */
export type UserWebResponse = UserWebResponseSuccess | UserWebResponseFailure;

