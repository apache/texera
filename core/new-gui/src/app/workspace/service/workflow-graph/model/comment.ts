import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import { WorkflowUtilService } from '../util/workflow-util.service';
import { Point, OperatorPredicate, OperatorLink } from '../../../types/workflow-common.interface';
import { WorkflowGraph } from './workflow-graph';
import { JointGraphWrapper } from './joint-graph-wrapper';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { environment } from './../../../../../environments/environment';
import { OperatorStatistics } from 'src/app/workspace/types/execute-workflow.interface';

export interface Comment extends Readonly<{
  commentID: string,
  content: string[],
  attachedOperatorID: string,
}>{}
