
import { FileItem } from 'ng2-file-upload';

export interface UserDictionary {
  id: string;
  name: string;
  items: string[];
  description?: string;
}

export interface SavedManualDictionary extends Readonly<{
  name: string;
  content: string;
  separator: string;
}> { }

export interface SavedDictionaryResult extends Readonly<{
  command: number;
  savedQueue: FileItem[];
  savedManualDictionary: SavedManualDictionary;
}> { }
