import { Injectable } from '@angular/core';
import { UserService } from '../../../common/service/user/user.service';
import { User } from '../../../common/type/user';
import { Workflow } from '../../../common/type/workflow';
import { localGetObject, localSetObject } from '../../../common/util/storage';

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
  private static readonly LOCAL_STORAGE_KEY: string = 'workflow';

  constructor(
    private userService: UserService
  ) {

    // reset the cache upon change of user
    this.registerUserChange();
  }

  public getCachedWorkflow(): Workflow|undefined {
    const workflow: Workflow|undefined = localGetObject<Workflow>(WorkflowCacheService.LOCAL_STORAGE_KEY);
    if (workflow === undefined) {
      this.resetCachedWorkflow();
    }
    return workflow;
  }

  public resetCachedWorkflow() {
    this.setCacheWorkflow(undefined);
  }

  public setCacheWorkflow(workflow: Workflow|undefined): void {
    localSetObject(WorkflowCacheService.LOCAL_STORAGE_KEY, workflow);
  }

  private registerUserChange(): void {
    this.userService.userChanged().subscribe((user: User|undefined) => {
      if (user === undefined) {
        this.resetCachedWorkflow();
      }

    });
  }
}
