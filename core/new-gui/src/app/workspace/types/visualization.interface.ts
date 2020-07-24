/**
 * This file contains some type declaration for the VisualizationPanelContent component
*/

/**
 * ChartType records all supported chart type in the frontend.
 */
export type ChartType = 'pie' | 'bar' | 'word cloud';

/**
 * WordCloudTuple defines the data format for word cloud visualization.
 */
export interface WordCloudTuple extends Readonly<{
  word: string,
  count: number
}> {}

export interface DialogData {
  table: object[];
  chartType: ChartType;
}
