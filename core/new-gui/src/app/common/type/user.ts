/**
 * This interface stores the information about the user account.
 * These information is used to identify users and to save their data
 * Corresponds to `core/amber/src/main/scala/edu/uci/ics/texera/web/resource/auth/UserResource.scala`
 */
import {Point} from "../../workspace/types/workflow-common.interface";

export interface User
  extends Readonly<{
    name: string;
    uid: number;
    googleId?: string;
    color?: string;
    clientId?: string;
  }> {}

export interface UserState {
  user: User;
  isActive: boolean;
  userCursor: Point;
  highlighted?: string[];
  unhighlighted?: string[];
  currentlyEditing?: string;
  changed?: string;
  editingCode?: boolean;
}
