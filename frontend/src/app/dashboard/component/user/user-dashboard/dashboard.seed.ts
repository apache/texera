/**
 * The seed "Diabetes Prediction Results" dashboard shown on first visit.
 */

import { Dashboard, DashboardWidget } from "./dashboard.types";

export function buildSeedDashboard(genId: () => string): Dashboard {
  const widgets: DashboardWidget[] = [
    {
      id: genId(),
      layout: { x: 0, y: 0, w: 3, h: 2 },
      widget: {
        type: "metric",
        config: { title: "Total Samples", value: "768", caption: "Pima Indians dataset", color: "#4cc9f0" },
      },
    },
    {
      id: genId(),
      layout: { x: 3, y: 0, w: 3, h: 2 },
      widget: {
        type: "metric",
        config: { title: "Best Accuracy", value: "96.7%", caption: "Logistic Regression", color: "#52c41a" },
      },
    },
    {
      id: genId(),
      layout: { x: 6, y: 0, w: 3, h: 2 },
      widget: {
        type: "metric",
        config: { title: "Features", value: "8", caption: "After preprocessing", color: "#b37feb" },
      },
    },
    {
      id: genId(),
      layout: { x: 9, y: 0, w: 3, h: 2 },
      widget: {
        type: "metric",
        config: { title: "Winning Model", value: "LR", caption: "Logistic Regression", color: "#ff9c6e" },
      },
    },
    {
      id: genId(),
      layout: { x: 0, y: 2, w: 8, h: 4 },
      widget: {
        type: "bar",
        config: {
          title: "Model Comparison",
          categories: ["Logistic Reg.", "Random Forest", "Gradient Boost", "SVM", "KNN"],
          series: [
            { name: "Accuracy", color: "#4cc9f0", values: [96.7, 94.1, 95.2, 92.4, 89.6] },
            { name: "Precision", color: "#7c5cff", values: [95.8, 92.5, 94.0, 91.0, 87.4] },
            { name: "F1 Score", color: "#52c41a", values: [96.2, 93.2, 94.5, 91.6, 88.2] },
          ],
          yAxisLabel: "Score (%)",
          yMax: 100,
        },
      },
    },
    {
      id: genId(),
      layout: { x: 8, y: 2, w: 4, h: 4 },
      widget: {
        type: "donut",
        config: {
          title: "Class Distribution",
          segments: [
            { label: "Non-diabetic", value: 65, color: "#4cc9f0" },
            { label: "Diabetic", value: 35, color: "#f5587b" },
          ],
          centerLabel: "768 samples",
        },
      },
    },
    {
      id: genId(),
      layout: { x: 0, y: 6, w: 8, h: 4 },
      widget: {
        type: "hbar",
        config: {
          title: "Feature Importance",
          color: "#7c5cff",
          xMax: 1,
          items: [
            { label: "Glucose", value: 0.42 },
            { label: "BMI", value: 0.21 },
            { label: "Age", value: 0.14 },
            { label: "Insulin", value: 0.09 },
            { label: "Pregnancies", value: 0.06 },
            { label: "Blood Pressure", value: 0.04 },
            { label: "Skin Thickness", value: 0.02 },
            { label: "Pedigree", value: 0.02 },
          ],
        },
      },
    },
    {
      id: genId(),
      layout: { x: 8, y: 6, w: 4, h: 4 },
      widget: {
        type: "text",
        config: {
          title: "Key Findings",
          body:
            "• Logistic Regression outperforms Random Forest on this dataset, suggesting the decision boundary is largely linear once features are standardized.\n\n" +
            "• Glucose is the dominant predictor (≈42% importance), aligning with clinical literature.\n\n" +
            "• Class imbalance (65/35) is mild — no resampling needed.\n\n" +
            "• Next step: validate on the held-out 2024 cohort.",
        },
      },
    },
  ];

  return {
    id: "seed-diabetes",
    name: "Diabetes Prediction Results",
    description: "Demo: model comparison for diabetes prediction on the Pima Indians dataset.",
    createdAt: Date.now(),
    updatedAt: Date.now(),
    widgets,
  };
}
