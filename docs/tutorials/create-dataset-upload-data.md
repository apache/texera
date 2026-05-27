---
title: "Create Dataset, upload data to it and use it in Workflow"
weight: 20
---

This tutorial goes through the process of preparing data by creating dataset and creating a workflow to analyze data resided in the dataset using Texera. 

More specifically, we are going to create a dataset named `Sales Dataset` which contains a file about the sales data of different types of merchandises for several countries. And the workflow will calculate the average sales per item type across different countries in Europe from the [CountrySalesData.csv](statics/files/CountrySalesData.csv) (Make sure the downloaded file is in `.csv` file extension). The sales data has been downloaded from [eforexcel.com](http://eforexcel.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/) and has 100 rows of data. 

We will first be creating a dataset and uploading the sales data to it. Then we will be creating a workflow on Texera Web UI to 
  1. read the data from the file;
  2. filter the relevant data based on keywords;
  3. perform an aggregation.

**1. Upload data by creating a Dataset**
 * Go to the Dataset tab and click the `dataset creation` icon to start creating the datasaet
 * Name the dataset as `Sales Dataset`, drag and drop the `CountrySalesData.csv` to the file uploading area
 * Click `Create`, the dataset we just created, along with the preview of `CountrySalesData.csv` is shown.
![2024-03-05 22 00 43](/images/github-assets/e17631b3-bf58-442f-af19-00f0ab704acb.png)

**2. Read data in Workflow**  
 * On the left panel, go to the `environment` tab and click `Add Dataset` to add the `Sales Dataset` to current workflow. `CountrySalesData.csv` will be available to be previewed and loaded to the workflow.
![2024-03-05 22 26 45](/images/github-assets/45e98e6b-fe6a-405c-bd24-22ee28ee3716.png)'
 * Drag and drop a `CSV File Scan` operator. On the right panel, input the file name `CountrySalesData.csv` and select the path from the drop down menu
 * Run the workflow, you should be able to see the loaded sales data.
![2024-03-05 22 46 11](/images/github-assets/77389a4c-dd73-4179-b8c0-ebf10241b182.png)


**3. Add operators to analyze data**
 * Drag and drop a `Filter` operator to keep only the sales data in `Europe`
![2024-03-05 22 51 26](/images/github-assets/9b73fcaa-a7df-4efb-8189-4054a6bef527.png)

 * Drag and drop a `Aggregate` operator to get the average sold units group by `Item Type`
![2024-03-05 22 53 06](/images/github-assets/67ade74c-df20-44b1-a9fa-1b8edb4af0cf.png)

