# Project Background
Vendor-dependent businesses face recurring profit leakage due to inconsistent vendor performance. With over 120+ vendors, hundreds of product lines, and wide variability in pricing, delivery reliability, and sales throughput, understanding which vendors add value, which destroy margin, and where procurement risk is concentrated is critical.

This project analyzes vendor-level performance using sales, procurement, and margin metrics. With over 100+ active vendors, multiple brands, and varied purchase volumes, identifying which partners create value—and which erode profitability—is essential for operational efficiency.

This project uses analytics to:
* Evaluate vendor sales and profitability
* Diagnose operational inefficiencies (pricing, delays, low throughput)
* Identify strategic vs. low-value vendors
* Optimize bulk-purchase pricing
* Build an executive-level Power BI dashboard for procurement leadership
* Recommend interventions to improve margins and vendor selection

# Data Structure
The dataset Consists of 6 csv files :

<img width="500" height="450" alt="dataset ERD diagram" src="https://github.com/user-attachments/assets/704d6406-79a0-4cb6-b1ea-c6b9a564df34" />

After thorough review and evaluation, the entire dataset has been consolidated into a single CSV file: 

<img width="200" height="300" alt="Screenshot 2025-12-16 105113" src="https://github.com/user-attachments/assets/913962d2-a951-439a-b429-3bea6381c1ad" />

This Consolidated Table is used for Data Analysis to answer our questions

# Executive Summary
### Overview of Findings 
Analysis reveals that vendor performance is highly unequal: a small group of top vendors disproportionately drives financial outcomes, while a long tail of under-performing vendors creates inefficiencies.
* **Top 10 vendors contribute 65.69% of total purchase dollars**, while the remaining 109 vendors contribute only 34.31%. This highlights procurement risk and dependency, and signals an opportunity for renegotiation or vendor diversification.
* **Price Does Not Drive Sales or Profitability** : Correlation analysis shows that Purchase price vs. total sales dollars: –0.01 and Purchase price vs. gross profits: –0.02. These near-zero correlations indicate that price variation does not significantly influence sales revenue or profitability.
* **Bulk ordering leads to meaningful reductions in unit purchase price**, suggesting a clear cost-optimization strategy. Vendors respond favorably to larger orders, enabling procurement to leverage scale for improved margins.

