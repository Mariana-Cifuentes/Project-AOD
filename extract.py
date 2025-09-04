# extract.py
import pandas as pd
import os

def get_data():
    """
    Reads the CSV file and returns it as a pandas DataFrame.
    """
    data_dir = "data"
    csv_filename = "All_Sites_Times_Daily_Averages_AOD20.csv"
    csv_path = os.path.join(data_dir, csv_filename)

    df = pd.read_csv(csv_path)
    return df