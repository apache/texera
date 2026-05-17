/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { WorkflowHubEntry } from "./workflow-hub.types";

const HUB_ENTRIES_KEY = "texera_workflow_hub_entries_v1";
const HUB_STARS_KEY = "texera_workflow_hub_stars_v1";

const AVATAR_COLORS = ["#1677ff", "#52c41a", "#fa8c16", "#eb2f96", "#722ed1", "#13c2c2", "#fa541c", "#a0d911"];

const SEED_ENTRIES: WorkflowHubEntry[] = [
  {
    id: "diabetes-crisp-dm",
    authorName: "Aisha Patel",
    authorAvatarColor: AVATAR_COLORS[0],
    title: "Diabetes Prediction (CRISP-DM)",
    description:
      "End-to-end pipeline following the CRISP-DM methodology to predict diabetes from the Pima Indians dataset. Includes EDA, imputation, scaling, train/test split, logistic regression, and ROC evaluation.",
    category: "biomedical",
    tags: ["classification", "healthcare", "crisp-dm", "logistic-regression"],
    operators: [
      "CSVFileScan",
      "Filter",
      "Imputer",
      "StandardScaler",
      "TrainTestSplit",
      "LogisticRegression",
      "Predictor",
      "ConfusionMatrix",
      "ROC",
      "Aggregator",
      "Projection",
      "View",
      "Export",
    ],
    sampleOperators: ["CSVFileScan", "Filter", "Projection", "SklearnTrainingLogisticRegression", "SklearnPrediction"],
    stars: 412,
    forks: 87,
    views: 3120,
    featured: true,
    publishedAt: "2026-03-04T09:12:00Z",
    agent: { name: "Diabetes Agent", methodology: "CRISP-DM" },
  },
  {
    id: "sentiment-pipeline",
    authorName: "Marcus Chen",
    authorAvatarColor: AVATAR_COLORS[1],
    title: "Sentiment Analysis Pipeline",
    description:
      "Twitter sentiment classification with tokenization, stopword removal, TF-IDF features, and a linear SVM classifier. Includes per-class precision/recall reporting.",
    category: "nlp",
    tags: ["nlp", "sentiment", "tf-idf", "svm"],
    operators: ["CSVFileScan", "Tokenizer", "StopwordFilter", "TFIDF", "TrainTestSplit", "LinearSVM", "Predictor", "ClassificationMetrics", "View"],
    sampleOperators: ["CSVFileScan", "Projection", "HuggingFaceSentimentAnalysis", "BarChart"],
    stars: 298,
    forks: 64,
    views: 2140,
    featured: true,
    publishedAt: "2026-02-21T14:05:00Z",
  },
  {
    id: "iris-beginner",
    authorName: "Texera Team",
    authorAvatarColor: AVATAR_COLORS[2],
    title: "UCI Iris Beginner Template",
    description:
      "A friendly starter template for first-time Texera users. Loads the classic Iris dataset, trains a k-NN classifier, and visualizes the decision boundary.",
    category: "education",
    tags: ["beginner", "template", "knn", "iris"],
    operators: ["CSVFileScan", "Projection", "TrainTestSplit", "KNNClassifier", "Predictor", "ConfusionMatrix", "ScatterPlot", "View"],
    sampleOperators: ["CSVFileScan", "Projection", "SklearnTrainingDecisionTree", "SklearnPrediction", "Scatterplot"],
    stars: 612,
    forks: 203,
    views: 5430,
    featured: true,
    publishedAt: "2025-11-12T08:00:00Z",
  },
  {
    id: "credit-fraud",
    authorName: "Priya Krishnan",
    authorAvatarColor: AVATAR_COLORS[3],
    title: "Credit Card Fraud Detection",
    description:
      "Imbalanced classification on the Kaggle credit-card fraud dataset. Demonstrates SMOTE oversampling, isolation forest baseline, and gradient-boosted trees.",
    category: "finance",
    tags: ["fraud", "imbalanced", "smote", "gbm"],
    operators: [
      "CSVFileScan",
      "Filter",
      "Imputer",
      "SMOTE",
      "TrainTestSplit",
      "IsolationForest",
      "GradientBoosting",
      "Predictor",
      "ROC",
      "ConfusionMatrix",
      "View",
    ],
    sampleOperators: ["CSVFileScan", "Filter", "SklearnTrainingAdaptiveBoosting", "SklearnPrediction"],
    stars: 357,
    forks: 71,
    views: 2780,
    featured: false,
    publishedAt: "2026-01-30T11:42:00Z",
  },
  {
    id: "heart-disease",
    authorName: "Sofia Rossi",
    authorAvatarColor: AVATAR_COLORS[4],
    title: "Heart Disease Risk Analysis",
    description:
      "Cleveland heart disease dataset analyzed with EDA, correlation heatmap, feature selection, and a random forest classifier with SHAP-style feature importances.",
    category: "biomedical",
    tags: ["healthcare", "random-forest", "shap", "feature-importance"],
    operators: [
      "CSVFileScan",
      "Filter",
      "Imputer",
      "Projection",
      "Aggregator",
      "CorrelationHeatmap",
      "FeatureSelection",
      "TrainTestSplit",
      "RandomForest",
      "Predictor",
      "ConfusionMatrix",
      "FeatureImportance",
      "View",
      "Export",
      "Report",
    ],
    sampleOperators: ["CSVFileScan", "Filter", "Projection", "SklearnTrainingRandomForest", "SklearnPrediction"],
    stars: 289,
    forks: 52,
    views: 1980,
    featured: false,
    publishedAt: "2026-02-09T16:30:00Z",
    agent: { name: "Cardio Agent" },
  },
  {
    id: "movie-rec-eda",
    authorName: "Ethan Brown",
    authorAvatarColor: AVATAR_COLORS[5],
    title: "Movie Recommendation EDA",
    description:
      "Exploratory analysis of the MovieLens 1M dataset. Genre frequency, ratings distribution, user-segment cohorts, and a simple item-item collaborative filtering baseline.",
    category: "eda",
    tags: ["eda", "movielens", "collaborative-filtering"],
    operators: ["CSVFileScan", "Join", "Aggregator", "Histogram", "Projection", "Cohort", "ItemItemCF"],
    sampleOperators: ["CSVFileScan", "Filter", "Aggregate", "BarChart"],
    stars: 174,
    forks: 38,
    views: 1410,
    featured: false,
    publishedAt: "2026-03-12T19:00:00Z",
  },
  {
    id: "breast-cancer",
    authorName: "Lina Okafor",
    authorAvatarColor: AVATAR_COLORS[6],
    title: "Breast Cancer Classification",
    description:
      "Wisconsin Breast Cancer dataset classification. Compares logistic regression, SVM with RBF kernel, and a small neural net on the same train/test split.",
    category: "biomedical",
    tags: ["healthcare", "classification", "model-comparison"],
    operators: [
      "CSVFileScan",
      "Imputer",
      "StandardScaler",
      "TrainTestSplit",
      "LogisticRegression",
      "SVMClassifier",
      "NeuralNetClassifier",
      "Predictor",
      "ConfusionMatrix",
      "ROC",
      "ModelCompare",
      "View",
    ],
    sampleOperators: ["CSVFileScan", "Projection", "SklearnTrainingLogisticRegression", "SklearnPrediction", "Histogram"],
    stars: 246,
    forks: 49,
    views: 1820,
    featured: false,
    publishedAt: "2026-02-18T10:15:00Z",
  },
  {
    id: "covid-trial",
    authorName: "Daniel Park",
    authorAvatarColor: AVATAR_COLORS[7],
    title: "COVID-19 Clinical Trial Analysis",
    description:
      "Cohort analysis of a public COVID-19 clinical trial dataset. Survival curves, treatment-group comparison, and a Cox proportional hazards model.",
    category: "biomedical",
    tags: ["healthcare", "survival", "covid", "cox-ph"],
    operators: ["CSVFileScan", "Filter", "Cohort", "SurvivalCurve", "CoxPH", "Aggregator", "ForestPlot", "Report", "View", "Export"],
    sampleOperators: ["CSVFileScan", "Filter", "Aggregate", "LineChart"],
    stars: 198,
    forks: 41,
    views: 1530,
    featured: false,
    publishedAt: "2026-01-15T12:00:00Z",
  },
  {
    id: "titanic-survival",
    authorName: "Rachel Nguyen",
    authorAvatarColor: AVATAR_COLORS[0],
    title: "Titanic Survival Prediction",
    description:
      "Classic Kaggle Titanic walkthrough. Feature engineering for cabin/title, one-hot encoding, decision tree and ensemble baselines, leaderboard-style scoring.",
    category: "education",
    tags: ["beginner", "kaggle", "decision-tree", "ensemble"],
    operators: ["CSVFileScan", "Imputer", "OneHotEncoder", "FeatureEngineer", "TrainTestSplit", "DecisionTree", "RandomForest", "Predictor", "Score"],
    sampleOperators: ["CSVFileScan", "Filter", "SklearnTrainingDecisionTree", "SklearnPrediction"],
    stars: 521,
    forks: 184,
    views: 4710,
    featured: false,
    publishedAt: "2025-12-04T08:30:00Z",
  },
  {
    id: "stock-regression",
    authorName: "James Wright",
    authorAvatarColor: AVATAR_COLORS[1],
    title: "Stock Price Regression",
    description:
      "Time-series regression on daily S&P 500 closing prices. Lag features, train/test split with walk-forward validation, gradient boosting regressor, and RMSE/MAPE evaluation.",
    category: "finance",
    tags: ["time-series", "regression", "gbm", "finance"],
    operators: ["CSVFileScan", "TimeIndex", "LagFeatures", "TrainTestSplit", "GBMRegressor", "Predictor", "RMSE", "LinePlot"],
    sampleOperators: ["CSVFileScan", "Projection", "SklearnTrainingGradientBoosting", "SklearnPrediction", "LineChart"],
    stars: 233,
    forks: 56,
    views: 1690,
    featured: false,
    publishedAt: "2026-02-26T15:20:00Z",
  },
  {
    id: "mnist-digits",
    authorName: "Hiro Tanaka",
    authorAvatarColor: AVATAR_COLORS[2],
    title: "MNIST Digit Classification",
    description:
      "Image classification on MNIST handwritten digits. A small convolutional network trained end-to-end inside Texera, with a confusion matrix and sample predictions.",
    category: "cv",
    tags: ["computer-vision", "cnn", "mnist", "image-classification"],
    operators: ["ImageFolderScan", "Resize", "Normalize", "TrainTestSplit", "CNNClassifier", "Predictor", "ConfusionMatrix", "ImageGrid", "View", "Export"],
    sampleOperators: ["FileScan", "Projection", "SklearnTrainingMultiLayerPerceptron", "SklearnPrediction"],
    stars: 367,
    forks: 78,
    views: 2540,
    featured: false,
    publishedAt: "2026-03-01T17:45:00Z",
  },
  {
    id: "news-topic",
    authorName: "Olivia Schmidt",
    authorAvatarColor: AVATAR_COLORS[3],
    title: "News Topic Classification",
    description:
      "20-Newsgroups topic classification with TF-IDF features and multinomial naive Bayes. Demonstrates per-topic precision/recall and top-token explanations.",
    category: "nlp",
    tags: ["nlp", "topic", "naive-bayes", "tf-idf"],
    operators: ["CSVFileScan", "Tokenizer", "StopwordFilter", "TFIDF", "TrainTestSplit", "NaiveBayes", "Predictor", "ClassificationMetrics"],
    sampleOperators: ["CSVFileScan", "Projection", "SklearnTrainingComplementNaiveBayes", "SklearnPrediction"],
    stars: 162,
    forks: 31,
    views: 1280,
    featured: false,
    publishedAt: "2026-03-10T10:10:00Z",
  },
  {
    id: "air-quality",
    authorName: "Noah Williams",
    authorAvatarColor: AVATAR_COLORS[4],
    title: "Air Quality Time Series",
    description:
      "EPA air-quality readings analysis. Resampling, seasonality decomposition, anomaly flagging, and a per-station heatmap of PM2.5 levels.",
    category: "eda",
    tags: ["time-series", "eda", "anomaly", "environmental"],
    operators: ["CSVFileScan", "TimeIndex", "Resample", "SeasonalDecompose", "Anomaly", "Heatmap", "LinePlot", "View", "Export"],
    sampleOperators: ["CSVFileScan", "Filter", "Aggregate", "Histogram"],
    stars: 144,
    forks: 28,
    views: 1110,
    featured: false,
    publishedAt: "2026-02-13T13:00:00Z",
  },
  {
    id: "census-income",
    authorName: "Fatima Al-Hassan",
    authorAvatarColor: AVATAR_COLORS[5],
    title: "Census Income Prediction",
    description:
      "Predict whether adult income exceeds $50K from US Census features. Handles mixed categorical/numeric inputs, target encoding, and a calibrated XGBoost model.",
    category: "tabular",
    tags: ["tabular", "xgboost", "calibration", "census"],
    operators: [
      "CSVFileScan",
      "Imputer",
      "TargetEncoder",
      "StandardScaler",
      "TrainTestSplit",
      "XGBoost",
      "Calibration",
      "Predictor",
      "ROC",
      "ConfusionMatrix",
      "View",
    ],
    sampleOperators: ["CSVFileScan", "Filter", "SklearnTrainingGradientBoosting", "SklearnPrediction"],
    stars: 211,
    forks: 47,
    views: 1620,
    featured: false,
    publishedAt: "2026-01-22T09:45:00Z",
  },
  {
    id: "wine-quality",
    authorName: "Carlos Mendes",
    authorAvatarColor: AVATAR_COLORS[6],
    title: "Wine Quality Regression",
    description:
      "Predict wine quality scores from physicochemical features. A gentle introduction to regression with residual plots and feature importance.",
    category: "education",
    tags: ["beginner", "regression", "wine", "feature-importance"],
    operators: ["CSVFileScan", "StandardScaler", "TrainTestSplit", "LinearRegression", "Predictor", "ResidualPlot", "FeatureImportance"],
    sampleOperators: ["CSVFileScan", "Projection", "SklearnTrainingLogisticRegression", "SklearnPrediction", "Histogram"],
    stars: 188,
    forks: 60,
    views: 1450,
    featured: false,
    publishedAt: "2025-12-20T11:30:00Z",
  },
];

@Injectable({ providedIn: "root" })
export class WorkflowHubService {
  private entriesSubject = new BehaviorSubject<WorkflowHubEntry[]>(this.loadEntries());
  private starsSubject = new BehaviorSubject<Set<string>>(this.loadStars());

  public entries$(): Observable<WorkflowHubEntry[]> {
    return this.entriesSubject.asObservable();
  }

  public stars$(): Observable<Set<string>> {
    return this.starsSubject.asObservable();
  }

  public getEntries(): WorkflowHubEntry[] {
    return this.entriesSubject.value;
  }

  public getEntry(id: string): WorkflowHubEntry | undefined {
    return this.entriesSubject.value.find(e => e.id === id);
  }

  public isStarred(id: string): boolean {
    return this.starsSubject.value.has(id);
  }

  public toggleStar(id: string): boolean {
    const set = new Set(this.starsSubject.value);
    const entries = [...this.entriesSubject.value];
    const idx = entries.findIndex(e => e.id === id);
    if (idx < 0) {
      return false;
    }
    let starred: boolean;
    if (set.has(id)) {
      set.delete(id);
      entries[idx] = { ...entries[idx], stars: Math.max(0, entries[idx].stars - 1) };
      starred = false;
    } else {
      set.add(id);
      entries[idx] = { ...entries[idx], stars: entries[idx].stars + 1 };
      starred = true;
    }
    this.starsSubject.next(set);
    this.entriesSubject.next(entries);
    this.persistStars(set);
    this.persistEntries(entries);
    return starred;
  }

  public recordView(id: string): void {
    const entries = [...this.entriesSubject.value];
    const idx = entries.findIndex(e => e.id === id);
    if (idx < 0) return;
    entries[idx] = { ...entries[idx], views: entries[idx].views + 1 };
    this.entriesSubject.next(entries);
    this.persistEntries(entries);
  }

  public recordFork(id: string): void {
    const entries = [...this.entriesSubject.value];
    const idx = entries.findIndex(e => e.id === id);
    if (idx < 0) return;
    entries[idx] = { ...entries[idx], forks: entries[idx].forks + 1 };
    this.entriesSubject.next(entries);
    this.persistEntries(entries);
  }

  public publishEntry(partial: Omit<WorkflowHubEntry, "id" | "stars" | "forks" | "views" | "featured" | "publishedAt" | "authorAvatarColor"> & {
    authorAvatarColor?: string;
  }): WorkflowHubEntry {
    const entry: WorkflowHubEntry = {
      id: `pub-${Date.now()}`,
      stars: 0,
      forks: 0,
      views: 0,
      featured: false,
      publishedAt: new Date().toISOString(),
      authorAvatarColor: partial.authorAvatarColor || AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)],
      ...partial,
    };
    const entries = [entry, ...this.entriesSubject.value];
    this.entriesSubject.next(entries);
    this.persistEntries(entries);
    return entry;
  }

  private loadEntries(): WorkflowHubEntry[] {
    try {
      const raw = localStorage.getItem(HUB_ENTRIES_KEY);
      if (!raw) return [...SEED_ENTRIES];
      const parsed = JSON.parse(raw) as WorkflowHubEntry[];
      // Merge: keep any user-published entries plus the latest seed copies (in case seed evolves between sessions).
      const seedIds = new Set(SEED_ENTRIES.map(e => e.id));
      const userPublished = parsed.filter(e => !seedIds.has(e.id));
      const seedWithCounts = SEED_ENTRIES.map(s => {
        const prior = parsed.find(p => p.id === s.id);
        return prior ? { ...s, stars: prior.stars, forks: prior.forks, views: prior.views } : s;
      });
      return [...userPublished, ...seedWithCounts];
    } catch {
      return [...SEED_ENTRIES];
    }
  }

  private loadStars(): Set<string> {
    try {
      const raw = localStorage.getItem(HUB_STARS_KEY);
      if (!raw) return new Set();
      return new Set(JSON.parse(raw) as string[]);
    } catch {
      return new Set();
    }
  }

  private persistEntries(entries: WorkflowHubEntry[]): void {
    try {
      localStorage.setItem(HUB_ENTRIES_KEY, JSON.stringify(entries));
    } catch {
      /* localStorage unavailable — ignore */
    }
  }

  private persistStars(set: Set<string>): void {
    try {
      localStorage.setItem(HUB_STARS_KEY, JSON.stringify([...set]));
    } catch {
      /* localStorage unavailable — ignore */
    }
  }
}
