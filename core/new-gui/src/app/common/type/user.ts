/**
 * This interface stores the information about the user account.
 * These information is used to identify users and to save their data
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserResource.java`
 */
export interface User extends Readonly<{
  name: string;
  uid: number;
}> {
}

