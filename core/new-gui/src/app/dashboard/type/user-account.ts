/**
 * This interface stores the information about the user account.
 * These information is used to identify users and to save their data
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserAccountResource.java`
 */
export interface UserAccount extends Readonly<{
  userName: string;
  userID: number;
}> {}

/**
 * This interface is used for communication between frontend and background
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserAccountResource.java`
 */
export interface UserAccountResponse extends Readonly<{
  code: 0 | 1; // 0 represents success and 1 represents error
  userAccount: UserAccount;
  message: string;
}> {}
