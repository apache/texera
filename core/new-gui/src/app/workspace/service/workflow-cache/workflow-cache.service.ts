import { Injectable } from '@angular/core';
import { UserService } from '../../../common/service/user/user.service';
import { User } from '../../../common/type/user';
import { Workflow } from '../../../common/type/workflow';
import { localGetObject, localRemoveObject, localSetObject } from '../../../common/util/storage';

/**
 *  CacheWorkflowService is responsible for saving the existing workflow and
 *  reloading back to the JointJS paper when the browser refreshes.
 *
 * It will listens to all the browser action events to update the cached workflow plan.
 * These actions include:
 *  1. operator add
 *  2. operator delete
 *  3. link add
 *  4. link delete
 *  5. operator property change
 *  6. operator position change
 *
 * @author Simon Zhou
 */
@Injectable({
  providedIn: 'root'
})
export class WorkflowCacheService {
  private static readonly WORKFLOW_KEY: string = 'workflow';

  constructor(private userService: UserService) {
    // reset the cache upon change of user
    this.registerUserChangeClearCache();
  }

  public getCachedWorkflow(): Workflow|undefined {
    return localGetObject<Workflow>(WorkflowCacheService.WORKFLOW_KEY);

  }

  public resetCachedWorkflow() {
    localRemoveObject(WorkflowCacheService.WORKFLOW_KEY);
  }

  public setCacheWorkflow(workflow: Workflow|undefined): void {
    localSetObject(WorkflowCacheService.WORKFLOW_KEY, workflow);
  }

  private registerUserChangeClearCache(): void {
    this.userService.userChanged().subscribe((user: User|undefined) => {
      if (user === undefined) {
        this.resetCachedWorkflow();
      }

    });
  }
}
