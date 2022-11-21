export interface DashboardAdminUserEntry
  extends Readonly<{
    uid: number;
    name: string;
    email: string;
    googleId: string;
    permission: number;
  }> {}
