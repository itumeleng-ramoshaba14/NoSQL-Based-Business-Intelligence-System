# ================== 1. Imports ==================
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os, re, calendar, json, math
from datetime import date, timedelta

# ================== 2. Config ==================
INPUT_PATH = "C:/Users/User/Desktop/321 datases/merged.csv"
CLEAN_CSV_PATH = "C:/Users/User/Desktop/321 datases/merged_clean.csv"
OUTPUT_XLSX_PATH = "C:/Users/User/Desktop/321 datases/clearvue_outputs_with_charts.xlsx"
CHARTS_DIR = "C:/Users/User/Desktop/321 datases/charts"

# ================== 3. Load Data ==================
print("Loading data...")
df = pd.read_csv(INPUT_PATH, low_memory=False)
print(f"Loaded data shape: {df.shape}")

# ================== 4. Standardize Columns ==================
def to_snake(name):
    s = re.sub(r"[^\w\s]+", "_", str(name).strip())
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"_+", "_", s).strip("_").lower()

df.columns = [to_snake(c) for c in df.columns]
print(f"Columns after standardization: {len(df.columns)}")

# ================== 5. Clean Data ==================
print("Cleaning data...")

# Drop obvious index-like columns
for col in ["unnamed_0", "index", "unnamed"]:
    if col in df.columns:
        df = df.drop(columns=[col])

# Clean object columns
for c in df.select_dtypes(include="object").columns:
    df[c] = df[c].astype(str).str.strip().replace({"nan": np.nan, "None": np.nan, "null": np.nan})

# Convert numeric columns
numeric_cols = [
    "total_due","amt_current","amt_30_days","amt_60_days","amt_90_days",
    "amt_120_days","amt_150_days","amt_180_days","amt_210_days","amt_240_days",
    "quantity","total_line_price","last_cost","tot_payment","bank_amt","transtype_code"
]

for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# ================== 6. Date Handling ==================
print("Processing dates...")
for dcol in ["trans_date","deposit_date"]:
    if dcol in df.columns:
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

# Create event_date
if "trans_date" in df.columns:
    df["event_date"] = df["trans_date"]
elif "deposit_date" in df.columns:
    df["event_date"] = df["deposit_date"]
else:
    df["event_date"] = pd.NaT
    print("Warning: No date columns found")

# ================== 7. Financial Calendar ==================
print("Creating financial calendar...")

def last_day_of_month(y, m): 
    return date(y, m, calendar.monthrange(y, m)[1])

def last_weekday_of_month(y, m, weekday):
    d = last_day_of_month(y, m)
    while d.weekday() != weekday: 
        d -= timedelta(days=1)
    return d

def last_friday(y, m): 
    return last_weekday_of_month(y, m, 4)

def last_saturday(y, m): 
    return last_weekday_of_month(y, m, 5)

def financial_month_label(d):
    lf = last_friday(d.year, d.month)
    if d <= lf: 
        return d.year, d.month
    return (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)

def financial_bounds(y, m):
    py, pm = (y - 1, 12) if m == 1 else (y, m - 1)
    return last_saturday(py, pm), last_friday(y, m)

def financial_quarter_label(y, m):
    q = (m - 1) // 3 + 1
    return f"FY{y} Q{q}"

# Initialize financial columns properly
df["fin_month_label"] = None  # Use None for object type
df["fin_month_start"] = pd.NaT
df["fin_month_end"] = pd.NaT
df["fin_year"] = np.nan
df["fin_month_num"] = np.nan
df["fin_quarter"] = None

# Process financial calendar
date_mask = df["event_date"].notna()
print(f"Processing {date_mask.sum()} records with dates...")

for i, d in df.loc[date_mask, "event_date"].items():
    try:
        y, m = financial_month_label(d.date())
        start, end = financial_bounds(y, m)
        df.at[i, "fin_month_label"] = f"{y}-{m:02d}"
        df.at[i, "fin_month_start"] = start
        df.at[i, "fin_month_end"] = end
        df.at[i, "fin_year"] = y
        df.at[i, "fin_month_num"] = m
        df.at[i, "fin_quarter"] = financial_quarter_label(y, m)
    except Exception as e:
        print(f"Error processing date {d}: {e}")

# Convert to proper types
df["fin_month_label"] = df["fin_month_label"].astype(str)
df["fin_year"] = df["fin_year"].astype('Int64')  # Nullable integer
df["fin_month_num"] = df["fin_month_num"].astype('Int64')

# ================== 8. Derived Metrics ==================
print("Calculating derived metrics...")

# Average Selling Price
if set(["quantity","total_line_price"]).issubset(df.columns):
    df["asp"] = np.where(df["quantity"].fillna(0) != 0,
                        df["total_line_price"] / df["quantity"],
                        np.nan)

# Cost of Goods Sold and Gross Margin
if set(["last_cost","quantity"]).issubset(df.columns):
    df["cogs"] = df["last_cost"] * df["quantity"]
    if "total_line_price" in df.columns:
        df["gross_margin"] = df["total_line_price"] - df["cogs"]
        df["gm_pct"] = np.where(df["total_line_price"].abs() > 1e-9,
                               df["gross_margin"] / df["total_line_price"],
                               np.nan)

# ================== 9. Data Quality Flags ==================
print("Creating data quality flags...")

df["flag_negative_qty"] = (df["quantity"] < 0) if "quantity" in df.columns else False
df["flag_negative_price"] = (df["total_line_price"] < 0) if "total_line_price" in df.columns else False
df["flag_zero_price_nonzero_qty"] = False

if set(["total_line_price","quantity"]).issubset(df.columns):
    df["flag_zero_price_nonzero_qty"] = ((df["quantity"].fillna(0) != 0) & 
                                        (df["total_line_price"].fillna(0) == 0))

# ================== 10. Flexible Column Finder ==================
def find_column(patterns, df_columns):
    """Find first column that matches any pattern in the list"""
    for pattern in patterns:
        for col in df_columns:
            if pattern in col.lower():
                return col
    return None

# ================== 11. Aggregations ==================
print("Creating aggregations...")

# Find product description column dynamically
product_desc_col = find_column([
    'product_desc', 'product_description', 'prod_desc', 
    'description', 'product_name', 'prod_name'
], df.columns)

if not product_desc_col:
    product_desc_col = find_column(['product_code', 'product_id', 'prod_code'], df.columns)

print(f"Using '{product_desc_col}' for product aggregations")

# Sales by Month
sales_by_month = pd.DataFrame()
if 'fin_month_label' in df.columns and 'total_line_price' in df.columns:
    sales_by_month = df.groupby("fin_month_label", as_index=False).agg(
        revenue=("total_line_price", "sum")
    )
    
    if 'gross_margin' in df.columns:
        gm_agg = df.groupby("fin_month_label")["gross_margin"].sum().reset_index()
        sales_by_month = sales_by_month.merge(gm_agg, on="fin_month_label")
        sales_by_month["gm_pct"] = sales_by_month["gross_margin"] / sales_by_month["revenue"]

# Top Products
top_products = pd.DataFrame()
if product_desc_col and 'total_line_price' in df.columns:
    top_products = (
        df.groupby(product_desc_col, as_index=False)
        .agg(revenue=("total_line_price", "sum"))
        .sort_values("revenue", ascending=False)
        .head(15)
    )
    # Clean product names for charting
    top_products['product_short'] = top_products[product_desc_col].str[:30] + '...'

# Additional Aggregations
aggregations = {}

# Sales by Region and Brand
region_col = find_column(['region_desc', 'region', 'territory'], df.columns)
brand_col = find_column(['brand_desc', 'brand', 'brand_name'], df.columns)

if region_col and brand_col and 'fin_month_label' in df.columns:
    group_cols = ["fin_month_label", region_col, brand_col]
    sales_by_region_brand = df.groupby(group_cols, as_index=False).agg(
        revenue=("total_line_price", "sum"),
        units=("quantity", "sum") if "quantity" in df.columns else pd.NamedAgg(column="quantity", aggfunc="sum")
    )
    aggregations['sales_by_region_brand'] = sales_by_region_brand

# Accounts Receivable Aging
if 'total_due' in df.columns and region_col and 'fin_month_label' in df.columns:
    ar_by_region = df.groupby(["fin_month_label", region_col], as_index=False).agg(
        ar_total=("total_due", "sum")
    )
    aggregations['ar_by_region'] = ar_by_region

# ================== 12. Charts ==================
print("Creating charts...")
os.makedirs(CHARTS_DIR, exist_ok=True)

# Revenue Trend Chart
if not sales_by_month.empty and len(sales_by_month) > 1:
    plt.figure(figsize=(12, 6))
    plt.plot(sales_by_month["fin_month_label"], sales_by_month["revenue"], 
             marker='o', linewidth=2, markersize=6)
    plt.title("Revenue by Financial Month", fontsize=14, fontweight='bold')
    plt.xlabel("Financial Month")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/revenue_by_month.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Revenue chart created")
else:
    print("✗ Not enough data for revenue chart")

# Top Products Chart
if not top_products.empty and len(top_products) > 1:
    plt.figure(figsize=(12, 8))
    # Use shortened names for better display
    if 'product_short' in top_products.columns:
        labels = top_products['product_short']
    else:
        labels = top_products[product_desc_col].str[:25] + '...'
    
    plt.bar(range(len(top_products)), top_products["revenue"], color='skyblue', alpha=0.7)
    plt.title("Top Products by Revenue", fontsize=14, fontweight='bold')
    plt.xlabel("Products")
    plt.ylabel("Revenue")
    plt.xticks(range(len(top_products)), labels, rotation=45, ha='right')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/top_products.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Top products chart created")
else:
    print("✗ Not enough data for products chart")

# ================== 13. Export to Excel with Charts ==================
print("Exporting to Excel...")

try:
    with pd.ExcelWriter(OUTPUT_XLSX_PATH, engine="xlsxwriter") as writer:
        # Summary sheet
        summary_data = {
            "Metric": ["Total Rows", "Total Columns", "Date Range Start", "Date Range End", 
                      "Total Revenue", "Financial Months", "Products with Revenue"],
            "Value": [
                len(df),
                len(df.columns),
                df["event_date"].min() if not df["event_date"].isna().all() else "No dates",
                df["event_date"].max() if not df["event_date"].isna().all() else "No dates",
                df["total_line_price"].sum() if "total_line_price" in df.columns else "N/A",
                df["fin_month_label"].nunique() if "fin_month_label" in df.columns else "N/A",
                top_products.shape[0] if not top_products.empty else "N/A"
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="00_Summary", index=False)
        
        # Main data sheet
        df.to_excel(writer, sheet_name="01_Cleaned_Data", index=False)
        
        # Aggregation sheets
        if not sales_by_month.empty:
            sales_by_month.to_excel(writer, sheet_name="02_Revenue_by_Month", index=False)
        
        if not top_products.empty:
            top_products.to_excel(writer, sheet_name="03_Top_Products", index=False)
        
        # Additional aggregation sheets
        for sheet_name, agg_data in aggregations.items():
            if not agg_data.empty:
                clean_sheet_name = sheet_name.replace('_', ' ').title()[:31]
                agg_data.to_excel(writer, sheet_name=clean_sheet_name, index=False)
        
        # Data quality flags
        quality_issues = df[df["flag_negative_qty"] | df["flag_negative_price"] | df["flag_zero_price_nonzero_qty"]]
        if not quality_issues.empty:
            quality_issues.head(1000).to_excel(writer, sheet_name="04_Quality_Flags", index=False)
        
        # Add charts to Excel
        workbook = writer.book
        chart_files = [
            ("Revenue_Chart", f"{CHARTS_DIR}/revenue_by_month.png"),
            ("Top_Products_Chart", f"{CHARTS_DIR}/top_products.png")
        ]
        
        for sheet_name, img_path in chart_files:
            if os.path.exists(img_path):
                try:
                    worksheet = workbook.add_worksheet(sheet_name)
                    worksheet.insert_image("A1", img_path, {"x_scale": 0.8, "y_scale": 0.8})
                except Exception as e:
                    print(f"Could not add {sheet_name}: {e}")
    
    print("✓ Excel export completed")
    
except Exception as e:
    print(f"Error during Excel export: {e}")

# ================== 14. Save Cleaned CSV ==================
try:
    df.to_csv(CLEAN_CSV_PATH, index=False)
    print(f"✓ Cleaned CSV saved: {CLEAN_CSV_PATH}")
except Exception as e:
    print(f"Error saving CSV: {e}")

# ================== 15. JSONL Export for MongoDB ==================
print("Preparing JSONL exports for MongoDB...")

def export_jsonl(df_in, path):
    """Export DataFrame to JSONL format for MongoDB"""
    try:
        with open(path, "w", encoding="utf-8") as f:
            for _, row in df_in.iterrows():
                obj = {}
                for c, v in row.items():
                    if pd.isna(v):
                        obj[c] = None
                    elif isinstance(v, (np.floating, float)):
                        obj[c] = float(v)
                    elif isinstance(v, (np.integer, int)):
                        obj[c] = int(v)
                    elif isinstance(v, pd.Timestamp):
                        obj[c] = v.isoformat() if not pd.isna(v) else None
                    else:
                        obj[c] = str(v) if v is not None else None
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        print(f"✓ Exported {path}")
    except Exception as e:
        print(f"✗ Error exporting {path}: {e}")

# Define collections for MongoDB
base_dir = "C:/Users/User/Desktop/321 datases/"

# Sales lines collection
sales_cols = [c for c in [
    "doc_number","customer_number","product_code","product_desc","brand_desc",
    "region_code","region_desc","quantity","total_line_price","last_cost",
    "asp","cogs","gross_margin","gm_pct","transtype_code","transtype_desc",
    "inventory_code","rep_code","event_date","fin_week_label","fin_month_label",
    "fin_month_start","fin_month_end","fin_quarter"
] if c in df.columns]

if sales_cols:
    export_jsonl(df[sales_cols].copy(), f"{base_dir}collection_sales_lines.jsonl")

# Receivables collection
recv_cols = [c for c in [
    "customer_number","cust_desc","total_due","amt_current","amt_30_days","amt_60_days",
    "amt_90_days","amt_120_days","amt_150_days","amt_180_days","amt_210_days","amt_240_days",
    "fin_month_label","fin_month_start","fin_month_end","region_desc"
] if c in df.columns]

if recv_cols:
    export_jsonl(df[recv_cols].copy(), f"{base_dir}collection_receivables.jsonl")

# Payments collection  
pay_cols = [c for c in [
    "doc_number","customer_number","tot_payment","bank_amt","deposit_date",
    "event_date","fin_week_label","fin_month_label","fin_quarter","region_desc"
] if c in df.columns]

if pay_cols:
    export_jsonl(df[pay_cols].copy(), f"{base_dir}collection_payments.jsonl")

# ================== 16. Final Summary ==================
print("\n" + "="*50)
print("✅ PROCESSING COMPLETE")
print("="*50)
print(f"📊 Original data: {df.shape}")
print(f"💾 Cleaned CSV: {CLEAN_CSV_PATH}")
print(f"📈 Excel report: {OUTPUT_XLSX_PATH}")
print(f"🖼️  Charts folder: {CHARTS_DIR}")

# Data quality summary
if 'flag_negative_qty' in df.columns:
    neg_qty = df['flag_negative_qty'].sum()
    neg_price = df['flag_negative_price'].sum() if 'flag_negative_price' in df.columns else 0
    zero_price = df['flag_zero_price_nonzero_qty'].sum() if 'flag_zero_price_nonzero_qty' in df.columns else 0
    
    print(f"🔍 Data Quality Issues:")
    print(f"   - Negative quantities: {neg_qty}")
    print(f"   - Negative prices: {neg_price}") 
    print(f"   - Zero price with quantity: {zero_price}")

print(f"📅 Financial months: {df['fin_month_label'].nunique() if 'fin_month_label' in df.columns else 'N/A'}")
print(f"💰 Total revenue: {df['total_line_price'].sum() if 'total_line_price' in df.columns else 'N/A':,.2f}")

print("\n--- MongoDB Import Commands ---")
print("mongoimport --db clearvue_proto --collection sales_lines --file collection_sales_lines.jsonl --type json")
print("mongoimport --db clearvue_proto --collection receivables --file collection_receivables.jsonl --type json") 
print("mongoimport --db clearvue_proto --collection payments --file collection_payments.jsonl --type json")
print("="*50)