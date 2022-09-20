import pandas as pd

data = pd.read_csv("test.csv")
data["quantity_sold"] = pd.to_numeric(data["quantity_sold"], errors='coerce').fillna(0).astype(int)
print(data["quantity_sold"])

data.to_csv('final_data.csv', index=False, na_rep='Unknown')