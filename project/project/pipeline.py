
   import pandas as pd
import sqlite3
import os

def download_and_process_cdi_data():
    # Download CDI data
    cdi_url = 'https://chronicdata.cdc.gov/api/views/g4ie-h725/rows.csv?accessType=DOWNLOAD'
    cdi_df = pd.read_csv(cdi_url, low_memory=False)
    cdi_df.dropna(subset=['DataValue'], inplace=True)
    cdi_df['DataValue'] = pd.to_numeric(cdi_df['DataValue'], errors='coerce')
    
    # Additional transformations if necessary
    # For example, filter for specific indicators or years
    # cdi_df = cdi_df[cdi_df['Year'] >= 2010]
    
    return cdi_df

def download_and_process_air_quality_data():
    # Download Air Quality data
    air_quality_url = 'https://data.cityofnewyork.us/api/views/c3uy-2p5r/rows.csv?accessType=DOWNLOAD'
    aq_df = pd.read_csv(air_quality_url)
    
    # Data transformation and cleaning
    # Drop rows with missing values
    aq_df.dropna(inplace=True)
    
    # Convert data types if necessary
    # For example, ensure numeric columns are correct
    numeric_columns = ['OZONE', 'PM25', 'NO2', 'SO2', 'CO']
    for col in numeric_columns:
        if col in aq_df.columns:
            aq_df[col] = pd.to_numeric(aq_df[col], errors='coerce')
    
    # Additional transformations if necessary
    # For example, parse dates or filter data
    # aq_df['Date'] = pd.to_datetime(aq_df['Date'], format='%Y-%m-%d')
    
    return aq_df

def save_data_to_sqlite(df, db_name, table_name):
    # Determine the data directory
    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    db_path = os.path.join(data_dir, db_name)
    conn = sqlite3.connect(db_path)
    
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to {db_path}")

if __name__ == '__main__':
    # Process and save CDI data
    print("Processing U.S. Chronic Disease Indicators (CDI) data...")
    cdi_df = download_and_process_cdi_data()
    save_data_to_sqlite(cdi_df, 'cdi_data.db', 'cdi')
    
    # Process and save Air Quality data
    print("Processing Air Quality data...")
    aq_df = download_and_process_air_quality_data()
    save_data_to_sqlite(aq_df, 'air_quality_data.db', 'air_quality')
    
    print("Data processing and saving completed.")
