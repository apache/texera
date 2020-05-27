/**
 * This interface stores the information about the users' dictionaries
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserDictionaryResource.java`
 */
export interface UserDictionary {
  id: number;
  name: string;
  items: string[];
  description: string;
}

/**
 * This interface stores the information about the users' dictionaries when uploading.
 * These information is used to upload the dictionary files to the backend.
 */
export interface DictionaryUploadItem {
  file: File;
  name: string;
  description: string;
}

/**
 * This interface stores the dictionary manually created by user
 * These information is used to upload to the backend.
 * Corresponds to `/web/src/main/java/edu/uci/ics/texera/web/resource/UserDictionaryResource.java`
 */
export interface ManualDictionary {
  name: string;
  content: string;
  separator: string;
  description: string;
}
