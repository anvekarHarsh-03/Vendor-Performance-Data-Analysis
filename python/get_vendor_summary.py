import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from ingestion_db import ingest_db

def create_vendor_summary(engine):
    vendor_sales_summary = pd.read_sql("""
            WITH FreightSummary AS ( 
                SELECT 
                    VendorNumber,
                    SUM(Freight) as FrieghtCost 
                    from vendor_invoice 
                    group by VendorNumber
            ),
            PurchaseSummary AS (
                SELECT
                    p.VendorNumber,
                    p.VendorName,
                    p.Brand,
                    p.PurchasePrice,
                    p.Description,
                    pp.Volume,
                    pp.Price as ActualPrice,
                    SUM(p.Quantity) as TotalPurchaseQuantity,
                    SUM(p.Dollars) as TotalPurchaseDollars
                FROM purchases p
                JOIN purchase_prices pp
                ON p.Brand = pp.Brand
                WHERE p.PurchasePrice > 0
                GROUP BY p.VendorNumber, p.VendorName, p.Brand
                ORDER BY TotalPurchaseDollars
            ),
            SalesSummary AS (
                SELECT
                    VendorNo,
                    Brand,
                    SUM(SalesDollars) as TotalSalesDollars,
                    SUM(SalesPrice) as TotalSalesPrice,
                    SUM(SalesQuantity) as TotalSalesQuantity,
                    SUM(ExciseTax) as TotalExciseTax
                FROM sales
                GROUP BY VendorNo, Brand
            )
        
        SELECT 
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.FrieghtCost
        FROM PurchaseSummary ps 
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs 
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC    
        """,con=engine)
    
    return vendor_sales_summary

def clean_data(vendor_sales_summary):
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float64')
    vendor_sales_summary['VendorName'] = vendor_sales_summary['VendorName'].str.strip()
    vendor_sales_summary.fillna(0, inplace=True)

    #creating new columnns for better analysis]
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['Gross Profit']/vendor_sales_summary['TotalSalesDollars'])*100 
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] - vendor_sales_summary['TotalPurchaseQuantity'] 
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    return vendor_sales_summary

if __name__=="__main__":

    engine = create_engine("mysql+pymysql://root:root@localhost:3306/vendor_performance_db")

    summary_df = create_vendor_summary(engine)

    clean_df = clean_data(summary_df)

    ingest_db(clean_df, 'vendor_sales_summary', engine)




        


