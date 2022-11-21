export interface DashboardUserEntry
  extends Readonly<{
    uid: number;
    email: string;
    accessLevel: number;
  }> {}

/**
 * This enum type helps indicate the method in which DashboardUserEntry[] is sorted
 */
export enum SortMethod {
  NameAsc,
  NameDesc,
  UploadTimeAsc,
  UploadTimeDesc,
}
