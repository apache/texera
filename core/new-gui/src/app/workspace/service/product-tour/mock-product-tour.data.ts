import { IStepOption } from 'ngx-tour-ng-bootstrap';

/**
 * Exports constants related to product-tour steps for testing purposes.
 *
 */

 export const mockTourSteps: IStepOption[] = [{
    anchorId: 'test1',
    content: `
    Test Step One
    `,
    placement: 'bottom',
    title: 'Step One',
  },
  {
    anchorId: 'test2',
    content: `
    Test Step Two
    `,
    placement: 'right',
    title: 'Step Two',
  },
  {
    anchorId: 'test3',
    content: `
    Test Step Three
    `,
    title: 'Step Three',
    placement: 'right',
 }];
