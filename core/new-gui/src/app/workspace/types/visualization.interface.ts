/**
 * This file contains some type declaration for the VisualizationPanelContent component
*/

/**
 * ChartType records all supported chart type in the frontend.
 */
export type ChartType = 'pie' | 'donut' | 'bar' | 'stacked bar' | 'word cloud'  | 'line' | 'spline';

/**
 * WordCloudTuple defines the data format for word cloud visualization.
 */
export interface WordCloudTuple extends Readonly<{
  word: string,
  count: number
}> {}

/**
 * DialogData defines the data format which passed to VisualizationPanelContent component
 */
export interface DialogData {
  table: object[];
  chartType: ChartType;
}
