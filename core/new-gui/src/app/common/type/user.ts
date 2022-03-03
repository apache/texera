/**
 * This interface stores the information about the user account.
 * These information is used to identify users and to save their data
 * Corresponds to `core/amber/src/main/scala/edu/uci/ics/texera/web/resource/auth/UserResource.scala`
 */
export interface User
  extends Readonly<{
    name: string;
    uid: number;
    googleId?: string;
  }> {}
