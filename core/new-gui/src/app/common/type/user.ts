import { Point } from "../../workspace/types/workflow-common.interface";

/**
 * This interface stores the information about the user account.
 * Such information is used to identify users and to save their data
 * Corresponds to `core/amber/src/main/scala/edu/uci/ics/texera/web/resource/auth/UserResource.scala`
 */
export interface User
  extends Readonly<{
    name: string;
    uid: number;
    googleId?: string;
    color?: string;
  }> {}

/**
 * Coeditor extends User and adds clientId to differentiate local user and collaborative editor
 */
export interface Coeditor extends User {
  clientId: string;
}

/**
 * This interface is for user-presence information in shared-editing.
 */
export interface CoeditorState {
  user: Coeditor;
  isActive: boolean;
  userCursor: Point;
  highlighted?: readonly string[];
  unhighlighted?: readonly string[];
  currentlyEditing?: string;
  changed?: string;
  editingCode?: boolean;
}
