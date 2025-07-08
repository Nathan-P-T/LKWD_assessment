# Senior Data Analyst Take-Home Task

This repo contains my submission for the LKWD Senior Data Analyst take-home assessment. 

## Folder structure
This repo includes the following files and outputs:
| File / Folder              | Description |
|---------------------------|-------------|
| `SQL task.py`/`SQL task.sql` | SQL solutions to part one of the task. Originally run in Python using `sqlalchemy`, I’ve also included as a separate SQL file (the core logic is the same) |
| `EDA.py`                  | Light exploratory data analysis (to quickly assess column viability), along with CVI* calculation and aggregation |
| `Report.docx`             | Main analysis report answering the business questions and giving some high-level recommendations. Please note the appendix may be hidden under 'more pages' |
| `Excel_workings.xlsx`     | Used to build two quick forecasting models (linear regression and historical scaling) along with charts used in the report |
| `Python_outputs/`         | Output CSVs for CVI aggregations along with EDA summaries (including auto-generated bar and distribution plots for data quality sense checks) |

*Customer Value Index (CVI) is a measure of how much a someone spends relative to the average (identifying potentially high-value customers regardless of volume)

## Analysis report
The main report focuses on:
- Overall business performance and year-to-date (YTD) sales for 2005
- Forecasting full-year sales using two simple methods
- High-level analysis of factors that influence LTV, such as:
	- Product line
	- Territory 
	- CVI
- Some high-level recommendations for strategic focus

Areas such as disrupted orders (which I defined as sales that were cancelled, on-hold, or disputed) were scoped but not included in the final write-up due to the time limit

## Tooling choices

Excel was used for quick chart production as well as forecasting experiments, like fitting a regression model with dummy variables for each month, as it was the simplest way to test ideas.

Python was used to quickly assess the quality of each column (high level metrics such as completeness), as well as to generate CVI aggregates in a reusable manner

## What I’d explore next
A few follow-on areas I would tackle are:
- Identifying seasonal patterns
- More analysis using indexes, such as:
	- Item popularity Index (quantity sold / quantity of all items sold) to give a high level indication of individual product performance
	- Revenue per unit index (total sales/quantity sold) in case items have variable pricing 
	- Item penetration Index (sales of item in region/ total regional sales) / (sales of item overall/total sales overall) to highlight how well products may sell in a territory versus how they do overall
- Extending the Python code to support automation (more modular, tested, functions, including logging and profiling to monitor performance)
- Utilising dashboarding software (e.g. in PowerBI) to explore the data more interactively and to allow for automated reporting
- Profiling individual customers (potentially identifying distinct groups by their behaviour)
- Assessing more granular variables (such as country, or city level aggregations) as well as relationships between variables (for example, product line by country)

