/**
 * This file contains some type declaration for the VisualizationPanelContent component
 */

/**
 * ChartType records all supported chart type in the frontend.
 */
export enum ChartType {
  PIE = 'pie',
  DONUT = 'donut',
  BAR = 'bar',
  STACKED_BAR = 'stacked bar',
  WORD_CLOUD = 'word cloud',
  LINE = 'line',
  SPLINE = 'spline'
}

/**
 * WordCloudTuple defines the data format for word cloud visualization.
 */
export interface WordCloudTuple extends Readonly<{
  word: string,
  count: number
}> {}
