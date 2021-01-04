import { Injectable } from '@angular/core';
import { UserService } from '../../../common/service/user/user.service';
import { User } from '../../../common/type/user';
import { Workflow } from '../../../common/type/workflow';
import { localGetObject, localRemoveObject, localSetObject } from '../../../common/util/storage';

@Injectable({
  providedIn: 'root'
})
export class WorkflowCacheService {
  private static readonly WORKFLOW_KEY: string = 'workflow';

  constructor(
    private userService: UserService
  ) {
    // reset the cache upon change of user
    this.registerUserChangeClearCache();
  }

  public getCachedWorkflow(): Workflow | undefined {
    return localGetObject<Workflow>(WorkflowCacheService.WORKFLOW_KEY);
  }

  public resetCachedWorkflow() {
    localRemoveObject(WorkflowCacheService.WORKFLOW_KEY);
  }

  public setCacheWorkflow(workflow: Workflow | undefined): void {
    localSetObject(WorkflowCacheService.WORKFLOW_KEY, workflow);
  }

  private registerUserChangeClearCache(): void {
    this.userService.userChanged().subscribe((user: User | undefined) => {
      if (user === undefined) {
        this.resetCachedWorkflow();
      }
    });
  }
}
