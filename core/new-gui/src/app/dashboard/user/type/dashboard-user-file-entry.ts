export interface DashboardUserFileEntry
  extends Readonly<{
    ownerName: string;
    file: UserFile;
    accessLevel: string;
    isOwner: boolean;
    projectIDs: number[];
  }> {}

/**
 * This interface stores the information about the users' files.
 * These information is used to locate the file for the operators.
 * Corresponds to `src/main/scala/edu/uci/ics/texera/web/resource/dashboard/file/UserFileResource.scala` (backend);
 * and `core/scripts/sql/texera_ddl.sql`, table `file` (database).
 */
export interface UserFile
  extends Readonly<{
    fid: number;
    name: string;
    path: string;
    size: number;
    description: string;
    uploadTime: string;
  }> {}

/**
 * This interface stores the information about the users' files when uploading.
 * These information is used to upload the file to the backend.
 */
export interface FileUploadItem {
  file: File;
  name: string;
  description: string;
  uploadProgress: number;
  isUploadingFlag: boolean;
}

/**
 * This enum type helps indicate the method in which DashboardUserFileEntry[] is sorted
 */
export enum SortMethod {
  NameAsc,
  NameDesc,
  SizeDesc,
  UploadTimeAsc,
  UploadTimeDesc,
}
