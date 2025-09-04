# main.py
import extract
import transform as t
import load

def main():
    print("Starting ETL Project - Data Warehouse\n")

    # 1. Extract
    df_raw = extract.get_data()   # reads CSV into DataFrame
    print(f"[Extract] Rows read: {len(df_raw):,}")

    # 2. Transform
    fact_df, dim_wavelength, dim_date, dim_site = t.transform_aerosoles(df_raw)
    print("[Transform] DataFrames created.")

    # 3. Load
    load.load_to_db(fact_df, dim_wavelength, dim_date, dim_site)
    print("[Load] Data successfully inserted into database.")

    print("\nETL process completed successfully.")

if __name__ == "__main__":
    main()
