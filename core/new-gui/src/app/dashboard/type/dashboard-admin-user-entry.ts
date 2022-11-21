export interface DashboardAdminUserEntry
  extends Readonly<{
    uid: number;
    name: string;
    accessLevel: number;
    permission: number;
  }> {}

/**
 * This enum type helps indicate the method in which DashboardAdminUserEntry[] is sorted
 */
export enum SortMethod {
  NameAsc,
  NameDesc,
  UploadTimeAsc,
  UploadTimeDesc,
}
