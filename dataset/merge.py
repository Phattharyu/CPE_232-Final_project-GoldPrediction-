import pandas as pd

# 1. โหลดข้อมูลรายเดือน
files = {
    "CPIAUCSL": "CPIAUCSL.csv",
    "CIVPART": "CIVPART.csv",
    "GDP": "GDP.csv",
    "GEPUCURRENT": "GEPUCURRENT.csv",
    "PPIACO": "PPIACO.csv",
    "UNRATE": "UNRATE (1).csv",
    "CLI": "USALOLITONOSTSAM.csv",
    "GDP_Reference": "USALORSGPNOSTSAM.csv"
}

dataframes = {}
for name, path in files.items():
    df = pd.read_csv(path)
    value_col = [col for col in df.columns if col != "observation_date"][0]
    df.rename(columns={value_col: name}, inplace=True)
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    dataframes[name] = df

# 2. รวมทั้งหมดแบบ merge ตาม observation_date
merged_df = dataframes.pop("CPIAUCSL")
for df in dataframes.values():
    merged_df = pd.merge(merged_df, df, on="observation_date", how="outer")

# 3. เติมค่า NaN ด้วยค่าก่อนหน้า
merged_df.sort_values("observation_date", inplace=True)
merged_df.fillna(method='ffill', inplace=True)

# 4. สร้างช่วงวันใหม่: รายวัน ตั้งแต่วันแรกสุดถึงวันสุดท้ายสุด
date_range = pd.date_range(start=merged_df["observation_date"].min(), 
                           end=merged_df["observation_date"].max(), 
                           freq='D')
daily_df = pd.DataFrame({"date": date_range})

# 5. ผูกวันที่ของ merged_df ให้เป็นช่วงรายวัน (map ค่าให้ทุกวันใช้ค่าของวันแรกของเดือนนั้น)
merged_df["month_key"] = merged_df["observation_date"].dt.to_period("M")
daily_df["month_key"] = daily_df["date"].dt.to_period("M")

# 6. ทำการ join โดยใช้ month_key
final_df = pd.merge(daily_df, merged_df.drop(columns="observation_date"), on="month_key", how="left")
final_df.drop(columns="month_key", inplace=True)

# กรองเฉพาะแถวที่ไม่มี NaN ทุกคอลัมน์
final_df.dropna(inplace=True)

# จัดระเบียบคอลัมน์
final_df = final_df[["date"] + [col for col in final_df.columns if col != "date"]]

# เซฟเป็น CSV
final_df.to_csv("daily_economic_data_filtered.csv", index=False)

# แสดงตัวอย่าง
print(final_df.head())
