# Layoffs Data Analysis

This project analyzes global layoff trends using a real-world dataset. The goal is to clean and preprocess the data using SQL, identify duplicates, and prepare the dataset for further insights into workforce reduction patterns by company, industry, and time.

## Project Files

- `layoffs.csv`: Raw dataset of company layoffs sourced from Kaggle.
- `Layoffs.sql`: SQL script for data cleaning and deduplication using window functions.

## Key Steps Performed

1. **Created a staging table** from the raw dataset to preserve original data.
2. **Identified potential duplicates** using `ROW_NUMBER()` and `PARTITION BY`.
3. **Manually validated** flagged duplicates to avoid accidental data loss.
4. Cleaned and structured the data for future analysis and visualization.

## 🛠Tools & Technologies

- SQL (MySQL/PostgreSQL compatible)
- Window Functions (`ROW_NUMBER`)
- Data Wrangling Techniques

## Future Scope

- Analyze layoff patterns over time and across industries.
- Visualize trends using Tableau, Power BI, or Python (matplotlib/seaborn).
- Identify most-affected companies and locations.

## Status

-Initial cleaning and SQL logic complete  
-Visualization and deeper analytics to be added

## Source

Dataset: [Kaggle - Layoffs 2022 Dataset](https://www.kaggle.com/datasets/swaptr/layoffs-2022)

---

