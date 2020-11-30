/**
 * This interface stores the information about the users' files.
 * These information is used to locate the file for the operators.
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserFileResource.java`
 */
export interface UserFile extends Readonly<{
  id: number;
  name: string;
  path: string;
  description: string;
  size: number;
}> { }

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
