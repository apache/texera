import { IStepOption } from 'ngx-tour-ng-bootstrap';

/**
 * Exports constants related to product-tour steps for testing purposes.
 *
 */

 export const mockTourSteps: IStepOption[] = [{
    anchorId: 'texera-navigation-grid-container',
    content: `
    Test Step One
    `,
    placement: 'bottom',
    title: 'Step One',
  },
  {
    anchorId: 'texera-operator-panel',
    content: `
    Test Step Two
    `,
    placement: 'right',
    title: 'Step Two',
  },
  {
    anchorId: 'texera-operator-panel',
    content: `
    Test Step Three
    `,
    title: 'Step Three',
    placement: 'right',
 }];
