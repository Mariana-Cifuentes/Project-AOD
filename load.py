# load.py
import os
import math
import sys

import pandas as pd
import mysql.connector as mysql
from mysql.connector import errorcode

# ---------------------------
# Configuration
# ---------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DB = os.getenv("MYSQL_DB", "aerosol_dw")

# Number of rows per batch for fact table inserts
BATCH_SIZE = 10_000


# ---------------------------
# Utilities
# ---------------------------
def connect_db():
    """
    Create and return a MySQL connection with autocommit disabled.
    Exit with a clear message if the schema does not exist or on errors.
    """
    try:
        conn = mysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            autocommit=False,
        )
        return conn
    except mysql.Error as e:
        if e.errno == errorcode.ER_BAD_DB_ERROR:
            sys.exit(f"Schema '{MYSQL_DB}' does not exist. Create it first in Workbench.")
        else:
            sys.exit(f"Error connecting to MySQL: {e}")


def run(cursor, sql: str):
    """Execute a single SQL statement."""
    cursor.execute(sql)


def create_schema_objects():
    """
    Create dimensional and fact tables if they do not exist.
    Commits DDL as a single transaction.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        run(cur, """
        CREATE TABLE IF NOT EXISTS dim_wavelength (
            id_wavelength INT PRIMARY KEY,
            Wavelength_nm DOUBLE NOT NULL,
            Spectral_Band VARCHAR(10),
            Sensitive_Aerosol VARCHAR(20),
            UNIQUE KEY uq_wavelength (Wavelength_nm)
        ) ENGINE=InnoDB;
        """)

        run(cur, """
        CREATE TABLE IF NOT EXISTS dim_date (
            id_date INT PRIMARY KEY,
            Date DATE NOT NULL,
            Year SMALLINT NOT NULL,
            Month TINYINT NOT NULL,
            Day TINYINT NOT NULL,
            Day_of_Year SMALLINT NOT NULL,
            UNIQUE KEY uq_date (Date)
        ) ENGINE=InnoDB;
        """)

        run(cur, """
        CREATE TABLE IF NOT EXISTS dim_site (
            id_site INT PRIMARY KEY,
            AERONET_Site VARCHAR(150),
            Latitude DECIMAL(9,6),
            Longitude DECIMAL(9,6),
            Elevation DOUBLE,
            Country VARCHAR(120),
            Continent VARCHAR(60),
            KEY ix_site_name (AERONET_Site),
            KEY ix_site_latlon (Latitude, Longitude)
        ) ENGINE=InnoDB;
        """)

        run(cur, """
        CREATE TABLE IF NOT EXISTS fact_aod (
            Fact_ID BIGINT PRIMARY KEY,
            id_date INT NOT NULL,
            id_wavelength INT NOT NULL,
            id_site INT NOT NULL,
            Particle_type VARCHAR(10),
            AOD_Value DOUBLE NOT NULL,
            Precipitable_Water DOUBLE NULL,
            Angstrom_Exponent DOUBLE NULL,
            KEY ix_date (id_date),
            KEY ix_wavelength (id_wavelength),
            KEY ix_site (id_site),
            CONSTRAINT fk_fact_date FOREIGN KEY (id_date) REFERENCES dim_date(id_date)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_fact_wavelength FOREIGN KEY (id_wavelength) REFERENCES dim_wavelength(id_wavelength)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_fact_site FOREIGN KEY (id_site) REFERENCES dim_site(id_site)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB;
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def truncate_tables():
    """
    Truncate all tables in the star schema in a safe order.
    Temporarily disables FK checks to avoid constraint issues.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        run(cur, "SET FOREIGN_KEY_CHECKS=0;")
        for tbl in ("fact_aod", "dim_wavelength", "dim_date", "dim_site"):
            run(cur, f"TRUNCATE TABLE {tbl};")
        run(cur, "SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _nan_to_none(val):
    """
    Normalize NaN/Inf values to None so they become NULLs in MySQL.
    """
    if isinstance(val, float) and (math.isnan(val) or val in (float("inf"), float("-inf"))):
        return None
    return None if pd.isna(val) else val


def insert_dim_wavelength(df: pd.DataFrame):
    """Bulk insert into dim_wavelength."""
    conn = connect_db()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO dim_wavelength (id_wavelength, Wavelength_nm, Spectral_Band, Sensitive_Aerosol)
        VALUES (%s, %s, %s, %s)
        """
        rows = [
            (
                int(r["id_wavelength"]),
                float(r["Wavelength_nm"]),
                _nan_to_none(r.get("Spectral_Band")),
                _nan_to_none(r.get("Sensitive_Aerosol")),
            )
            for _, r in df.iterrows()
        ]
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_dim_date(df: pd.DataFrame):
    """Bulk insert into dim_date."""
    conn = connect_db()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO dim_date (id_date, Date, Year, Month, Day, Day_of_Year)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                int(r["id_date"]),
                r["Date"].date() if pd.notna(r["Date"]) else None,
                int(r["Year"]),
                int(r["Month"]),
                int(r["Day"]),
                int(r["Day_of_Year"]),
            )
            for _, r in df.iterrows()
        ]
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_dim_site(df: pd.DataFrame):
    """Bulk insert into dim_site."""
    conn = connect_db()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO dim_site (
            id_site, AERONET_Site, Latitude, Longitude, Elevation, Country, Continent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                int(r["id_site"]),
                _nan_to_none(r.get("AERONET_Site")),
                _nan_to_none(r.get("Latitude")),
                _nan_to_none(r.get("Longitude")),
                _nan_to_none(r.get("Elevation")),
                _nan_to_none(r.get("Country")),
                _nan_to_none(r.get("Continent")),
            )
            for _, r in df.iterrows()
        ]
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_fact(df: pd.DataFrame):
    """
    Batch insert into fact_aod using chunks to control transaction size.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        sql = """
        INSERT INTO fact_aod (
            Fact_ID, id_date, id_wavelength, id_site,
            Particle_type, AOD_Value, Precipitable_Water, Angstrom_Exponent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        total = len(df)
        batches = (total // BATCH_SIZE) + (1 if total % BATCH_SIZE else 0)

        for b in range(batches):
            start = b * BATCH_SIZE
            end = min(start + BATCH_SIZE, total)
            chunk = df.iloc[start:end]

            rows = [
                (
                    int(r["Fact_ID"]),
                    int(r["id_date"]),
                    int(r["id_wavelength"]),
                    int(r["id_site"]),
                    _nan_to_none(r.get("Particle_type")),
                    float(r["AOD_Value"]),
                    _nan_to_none(r.get("Precipitable_Water")),
                    _nan_to_none(r.get("Angstrom_Exponent")),
                )
                for _, r in chunk.iterrows()
            ]

            cur.executemany(sql, rows)
            conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------------------------
# Function Load
# ---------------------------
def load_to_db(fact_df: pd.DataFrame,
               dim_wavelength: pd.DataFrame,
               dim_date: pd.DataFrame,
               dim_site: pd.DataFrame):
    """
    Full load process: create schema, truncate destination, and insert dimensions + fact.
    Designed to be called from main.py.
    """
    print("[Load] Creating schema objects...")
    create_schema_objects()
    print("[Load] Schema ready.")

    print("[Load] Truncating tables...")
    truncate_tables()
    print("[Load] Destination cleaned.")

    print("[Load] Inserting dimensions...")
    insert_dim_wavelength(dim_wavelength)
    insert_dim_date(dim_date)
    insert_dim_site(dim_site)
    print("[Load] Dimensions inserted.")

    print("[Load] Inserting fact table (batched)...")
    insert_fact(fact_df)
    print("[Load] Fact table inserted.")