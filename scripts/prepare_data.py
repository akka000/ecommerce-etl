import pandas as pd, os, json 
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) 
CONF = os.path.join(BASE, "config", "config.json") 

with open(CONF) as f:
    cfg = json.load(f)
raw = os.path.join(BASE, "data", "raw", cfg["local"]["default_raw"]) 
out_parquet = os.path.join(BASE, cfg["local"]["raw_parquet"])
 
df = pd.read_excel(raw, engine="openpyxl") 
 
df.columns = [c.strip() for c in df.columns]

dtype_map = {
    "Invoice": str,
    "StockCode": str,
    "Description": str,
    "Quantity": "Int64",        
    "InvoiceDate": str,
    "Price": float,
    "Customer ID": "Int64",
    "Country": str
}

for col, dtype in dtype_map.items():
    if col in df.columns:
        df[col] = df[col].astype(dtype)

df.to_parquet(out_parquet, index=False) 
print("Wrote parquet:", out_parquet) 