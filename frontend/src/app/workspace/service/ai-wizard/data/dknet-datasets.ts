/**
 * Curated dkNET-style biomedical datasets. The Pima Indians Diabetes dataset
 * comes pre-baked with a DataProfile so the LLM never has to guess column
 * names for the demo path (design-doc §4.2 Data Profiler).
 */

import { DknetDataset } from "../types";

export const DKNET_DATASETS: DknetDataset[] = [
  {
    id: "iris-example",
    name: "Iris Species (Texera example dataset)",
    description:
      "Classic 150-row Iris flower dataset, shipped with Texera's example loader (bin/single-node/examples/load-examples.sh). Use this for a quick end-to-end demo — path resolves on the backend without extra setup.",
    fileName: "/texera/iris/v1/Iris.csv",
    schema:
      "Id (integer), SepalLengthCm (float), SepalWidthCm (float), PetalLengthCm (float), PetalWidthCm (float), Species (string — Iris-setosa / Iris-versicolor / Iris-virginica). 150 rows.",
    profile: {
      rowCount: 150,
      source: "dknet-prebaked",
      columns: [
        { name: "Id", dtype: "int", nullRate: 0, uniqueCount: 150, sampleValues: ["1", "2", "3", "4", "5"] },
        {
          name: "SepalLengthCm",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 35,
          sampleValues: ["5.1", "4.9", "4.7", "4.6", "5.0"],
        },
        {
          name: "SepalWidthCm",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 23,
          sampleValues: ["3.5", "3.0", "3.2", "3.1", "3.6"],
        },
        {
          name: "PetalLengthCm",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 43,
          sampleValues: ["1.4", "1.4", "1.3", "1.5", "1.4"],
        },
        {
          name: "PetalWidthCm",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 22,
          sampleValues: ["0.2", "0.2", "0.2", "0.2", "0.2"],
        },
        {
          name: "Species",
          dtype: "str",
          nullRate: 0,
          uniqueCount: 3,
          sampleValues: ["Iris-setosa", "Iris-versicolor", "Iris-virginica", "Iris-setosa", "Iris-setosa"],
        },
      ],
    },
  },
  {
    id: "diabetes-cohort",
    name: "Pima Indians Diabetes",
    description:
      "768-patient cohort (Pima Indian heritage, NIDDK study). Source: Kaggle akshaydattatraykhare/diabetes-dataset, uploaded to Texera at /texera/diabetes/v1/dknet-diabetes.csv. Suitable for predictive modeling on the Outcome (binary diabetes diagnosis) target.",
    fileName: "/texera/diabetes/v1/dknet-diabetes.csv",
    schema:
      "Pregnancies (integer), Glucose (integer, mg/dL — 2-hr OGTT), BloodPressure (integer, diastolic mm Hg), SkinThickness (integer, triceps mm), Insulin (integer, 2-hr serum mu U/ml), BMI (float, kg/m²), DiabetesPedigreeFunction (float), Age (integer, years), Outcome (0/1, target label)",
    profile: {
      rowCount: 768,
      source: "dknet-prebaked",
      columns: [
        {
          name: "Pregnancies",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 17,
          sampleValues: ["6", "1", "8", "0", "3"],
        },
        {
          name: "Glucose",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 136,
          sampleValues: ["148", "85", "183", "89", "137"],
        },
        {
          name: "BloodPressure",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 47,
          sampleValues: ["72", "66", "64", "0", "40"],
        },
        {
          name: "SkinThickness",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 51,
          sampleValues: ["35", "29", "0", "23", "35"],
        },
        {
          name: "Insulin",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 186,
          sampleValues: ["0", "94", "168", "88", "543"],
        },
        {
          name: "BMI",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 248,
          sampleValues: ["33.6", "26.6", "23.3", "28.1", "43.1"],
        },
        {
          name: "DiabetesPedigreeFunction",
          dtype: "float",
          nullRate: 0,
          uniqueCount: 517,
          sampleValues: ["0.627", "0.351", "0.672", "0.167", "2.288"],
        },
        {
          name: "Age",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 52,
          sampleValues: ["50", "31", "32", "21", "33"],
        },
        {
          name: "Outcome",
          dtype: "int",
          nullRate: 0,
          uniqueCount: 2,
          sampleValues: ["1", "0", "1", "0", "1"],
        },
      ],
    },
  },
];
