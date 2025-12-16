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

<img width="1177" height="662" alt="PowerBI Dashboard" src="https://github.com/user-attachments/assets/1b68ca8d-d46c-409a-b5ae-d609aaa2d677" />


### Identifying Brands Requiring Pricing or Promotional Adjustments
These are the Brands showing Low sales performance but High profit margins and may benefit from targeted pricing realignment or promotional visibility to unlock untapped sales volume.

<img width="800" height="500" alt="sactterplot_for_lowPerformingBrands" src="https://github.com/user-attachments/assets/27eba02f-d42e-47a7-9a1e-ae55bb42a2f6" />

### Highest-Performing Vendors & Brands
Several vendors consistently outperform peers across:
* Sales quantity
* Sales dollars
* Gross profit contribution
These vendors form the core strategic group for long-term partnerships.

<img width="800" height="400" alt="Top10_vendors_and_brands_bySales" src="https://github.com/user-attachments/assets/7ee363f9-e4b4-49ec-b41c-bffa80a000d6" />

### Vendors With the Largest Financial Contribution
Ranking vendors by total purchase dollars highlights which partners drive majority procurement spending, enabling more focused contract management and negotiation strategies.
65.69% of total spending is concentrated within the top 10 vendors. This concentration poses both opportunity (volume leverage) and risk (over-dependency).

<img width="650" height="725" alt="Screenshot 2025-12-16 122656" src="https://github.com/user-attachments/assets/821ddddc-f986-4627-9727-2e1ac4796bf4" />


### Bulk Purchase Pricing Optimization
Bulk ordering reduces unit prices significantly.
Implications:
* Procurement teams can negotiate tiered pricing based on volume brackets.
* Vendors benefit from predictable demand; buyers benefit from cost reductions.
* Analytics can determine optimal order sizes for maximum savings.

# RECOMMENDATIONS
1. Consolidate or Renegotiate Underperforming Vendors
     * Vendors with low sales contribution, inconsistent pricing, or delivery inefficiencies should be deprioritized or consolidated.
2. Expand Strategic Partnerships With Top Vendors
     * Top-performing vendors justify: Preferential contracts, Increased procurement volume and Joint forecasting for volume-based discounts
3. Implement Bulk Purchasing Strategy
    * Formalize bulk order guidelines to ensure consistent access to lower unit prices.
4. Focus on Brands With High Margins but Low Throughput
   * Targeted discounting or marketing campaigns can unlock incremental revenue.

# Included Files
   [jupyter Notebook](VendorPerformanceAnalysis.ipynb) <br>
   [dataset](dataset)
