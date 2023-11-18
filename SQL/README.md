# SQL

## Project Goal
Create a model that predicts if a patient will have one or more ER visits in the following 6 months using longitudinal patient data from multiple db tables. 

## Submission Overview
### Exploratory Data Analysis
I first performed EDA on the 4 tables using SQL queries to understand the column values and types, number of unique Ids, and date ranges.

### Data Cleaning and Processing
To clean the data, I first consolidated columns in the `Procedure` and `Diagnosis` tables to produce less sparse columns for more effective model building. 
Next, I merged the 4 tables together, using `Patient Id` alone on the `Demographics` table and `Patient Id` and `Date` on the others.
I made the decision to create a binary outcome variable of whether each patient had 1 or more ER visits in the 6 months following a given date. To do this, I split the data into 4 6-month periods and aggregated the `ER_visits` column after grouping by `Patient Id` in each period. While this resulted in a reduction of longitudinal data, it provided a clear binary outcome variable and removed dependence of our data on specific Patient Ids and Dates, which could lead to overfitting. 
To validate the data, I performed checks on null values, feature correlation, and feature variance, and made changes to features as needed.

### Model Building
As a baseline model, I used a Logistic Regression model with no additional processing of the data. Due to the large class imbalance (skewed toward the 0 class), the model had a high overall accuraccy but very poor F1-score for the 1 class. 
To improve upon my baseline model, I attempted to correct for class imbalance by undersampling the majority class, using a tree-based model, and adding a boosting method to my model. These steps did significantly improve the F1-score of the 1 class, but of course decreased the overall model accuracy. Both models were also validated using 3-fold cross-validation.

## Future Steps
The current model could still be improved with additional feature engineering, hyperparameter tuning, and possibly the selection of another type of model. Additionally, given the time limit, I made some intentional choices of simplicity. It is possible the model could be improved by adding back the granularity of the time series data and using a forecasting method rather than a predictive model. 
