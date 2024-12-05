import os
import pandas as pd
import sqlite3
import pytest

@pytest.fixture
def data_dir(tmp_path):
    # Create a temporary data directory
    data = tmp_path / "data"
    data.mkdir()
    return str(data)

def test_download_and_process_cdi_data(mocker):
    # Mock the URL request to return a sample CSV content
    mock_csv = """Indicator,DataValue,Year
    Obesity,25.5,2020
    Diabetes,8.1,2020"""
    mocker.patch("pandas.read_csv", return_value=pd.read_csv(pd.compat.StringIO(mock_csv)))

    # Call the function
    cdi_df = download_and_process_cdi_data()

    # Check the DataFrame structure and values
    assert len(cdi_df) == 2
    assert "DataValue" in cdi_df.columns
    assert cdi_df["DataValue"].dtype == float

def test_download_and_process_air_quality_data(mocker):
    # Mock the URL request to return a sample CSV content
    mock_csv = """Date,OZONE,PM25,NO2,SO2,CO
    2020-01-01,0.03,12,23,4,0.9
    2020-01-02,0.02,11,19,3,0.7"""
    mocker.patch("pandas.read_csv", return_value=pd.read_csv(pd.compat.StringIO(mock_csv)))

    # Call the function
    aq_df = download_and_process_air_quality_data()

    # Check the DataFrame structure and values
    assert len(aq_df) == 2
    assert "OZONE" in aq_df.columns
    assert aq_df["OZONE"].dtype == float

def test_save_data_to_sqlite(data_dir):
    # Create a sample DataFrame
    data = {"Column1": [1, 2, 3], "Column2": ["A", "B", "C"]}
    df = pd.DataFrame(data)

    # Save to SQLite
    db_name = "test_data.db"
    table_name = "test_table"
    save_data_to_sqlite(df, db_name, table_name)

    # Verify the SQLite file exists
    db_path = os.path.join(data_dir, db_name)
    assert os.path.exists(db_path)

    # Verify the data is saved correctly
    conn = sqlite3.connect(db_path)
    saved_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    pd.testing.assert_frame_equal(df, saved_df)

def test_full_pipeline(mocker, data_dir):
    # Mock the CDI and Air Quality data processing functions
    mock_cdi_df = pd.DataFrame({
        "Indicator": ["Obesity", "Diabetes"],
        "DataValue": [25.5, 8.1],
        "Year": [2020, 2020]
    })
    mock_aq_df = pd.DataFrame({
        "Date": ["2020-01-01", "2020-01-02"],
        "OZONE": [0.03, 0.02],
        "PM25": [12, 11],
        "NO2": [23, 19],
        "SO2": [4, 3],
        "CO": [0.9, 0.7]
    })

    mocker.patch("pandas.read_csv", side_effect=[mock_cdi_df, mock_aq_df])

    # Run the pipeline functions
    cdi_df = download_and_process_cdi_data()
    save_data_to_sqlite(cdi_df, "cdi_data.db", "cdi")

    aq_df = download_and_process_air_quality_data()
    save_data_to_sqlite(aq_df, "air_quality_data.db", "air_quality")

    # Verify results
    cdi_db_path = os.path.join(data_dir, "cdi_data.db")
    aq_db_path = os.path.join(data_dir, "air_quality_data.db")

    assert os.path.exists(cdi_db_path)
    assert os.path.exists(aq_db_path)

    conn = sqlite3.connect(cdi_db_path)
    saved_cdi_df = pd.read_sql("SELECT * FROM cdi", conn)
    conn.close()
    pd.testing.assert_frame_equal(mock_cdi_df, saved_cdi_df)

    conn = sqlite3.connect(aq_db_path)
    saved_aq_df = pd.read_sql("SELECT * FROM air_quality", conn)
    conn.close()
    pd.testing.assert_frame_equal(mock_aq_df, saved_aq_df)
