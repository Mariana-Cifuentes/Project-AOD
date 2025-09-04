# transform.py
import os
import re
import zipfile
import requests
import pandas as pd
import numpy as np

# Optional import: GeoPandas for spatial enrichment
try:
    import geopandas as gpd
    _HAS_GPD = True
except Exception:
    _HAS_GPD = False

# =============================
# Configuration and constants
# =============================
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Natural Earth 1:50m – countries shapefile
NE_URL = "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_0_countries.zip"
NE_BASENAME = "ne_50m_admin_0_countries"
NE_ZIP_PATH = os.path.join(DATA_DIR, f"{NE_BASENAME}.zip")
NE_SHP_PATH = os.path.join(DATA_DIR, f"{NE_BASENAME}.shp")
NE_SIDEKICKS = [".shp", ".dbf", ".shx", ".prj"]

def _ne_shapefile_present(basename: str, folder: str) -> bool:
    # Check if all required shapefile components exist (.shp, .dbf, .shx, .prj)
    return all(os.path.exists(os.path.join(folder, basename + ext)) for ext in NE_SIDEKICKS)

def _ne_download_zip(url: str, out_path: str):
    # Download Natural Earth shapefile ZIP
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"Downloading: {url}")  # Prints: "Downloading: <url>"
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
    print("Download OK")  # Prints after successful download

def _ensure_ne_countries():
    # Ensure shapefile is available: download and extract if needed
    if _ne_shapefile_present(NE_BASENAME, DATA_DIR):
        return
    if not os.path.exists(NE_ZIP_PATH):
        _ne_download_zip(NE_URL, NE_ZIP_PATH)
    print("Extracting shapefile...")  # Prints before extraction
    with zipfile.ZipFile(NE_ZIP_PATH, "r") as z:
        z.extractall(DATA_DIR)

# =============================
# Aerosol classification rules
# =============================
AE_FINE_TH = 1.5   # Values >= 1.5 → fine particles
AE_COARSE_TH = 1.0 # Values <= 1.0 → coarse particles

def _classify_particle_type(ae: float) -> str | float:
    # Classify aerosol particle type based on Angstrom Exponent
    if pd.isna(ae):
        return np.nan
    if ae >= AE_FINE_TH:
        return "fine"
    if ae <= AE_COARSE_TH:
        return "coarse"
    return "mixed"

def _spectral_band(wavelength_nm: float) -> str:
    # Assign spectral band by wavelength
    if wavelength_nm < 400:
        return "UV"
    if wavelength_nm <= 700:
        return "VIS"
    return "NIR"

def _sensitive_aerosol(wavelength_nm: float) -> str:
    # Define sensitivity to fine/coarse particles by wavelength
    if wavelength_nm <= 500:
        return "fine-sensitive"
    if wavelength_nm >= 800:
        return "coarse-sensitive"
    return "balanced"

# =============================
# Site dimension enrichment
# =============================
def _build_dim_site_with_country_continent(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Build the site dimension table with Latitude/Longitude/Elevation,
    and enrich it with Country and Continent using GeoPandas + Natural Earth.
    """
    site_cols = ["AERONET_Site", "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"]
    available_cols = [c for c in site_cols if c in df_long.columns]

    # Base: unique sites
    dim_site = (
        df_long[available_cols]
        .drop_duplicates()
        .rename(columns={
            "Site_Latitude(Degrees)": "Latitude",
            "Site_Longitude(Degrees)": "Longitude",
            "Site_Elevation(m)": "Elevation"
        })
        .reset_index(drop=True)
    )

    # Clean coordinates (swap lat/long if values are out of expected ranges)
    dim_site["Latitude"]  = pd.to_numeric(dim_site["Latitude"], errors="coerce")
    dim_site["Longitude"] = pd.to_numeric(dim_site["Longitude"], errors="coerce")
    swap = (dim_site["Latitude"].abs() > 90) & (dim_site["Longitude"].abs() <= 90)
    dim_site.loc[swap, ["Latitude","Longitude"]] = dim_site.loc[swap, ["Longitude","Latitude"]].values
    dim_site = dim_site[
        dim_site["Latitude"].between(-90,90) & dim_site["Longitude"].between(-180,180)
    ].reset_index(drop=True)

    # Add unique site ID
    dim_site["id_site"] = np.arange(1, len(dim_site)+1)

    # Enrich with country and continent using shapefile
    try:
        if not _HAS_GPD:
            raise RuntimeError("GeoPandas not available")
        _ensure_ne_countries()

        # Create GeoDataFrame with site coordinates
        g_sites = gpd.GeoDataFrame(
            dim_site,
            geometry=gpd.points_from_xy(dim_site["Longitude"], dim_site["Latitude"]),
            crs="EPSG:4326"
        )

        # Load Natural Earth countries
        world = gpd.read_file(NE_SHP_PATH).to_crs("EPSG:4326")
        if "ADMIN" in world.columns:
            world = world.rename(columns={"ADMIN": "Country"})
        if "CONTINENT" in world.columns:
            world = world.rename(columns={"CONTINENT": "Continent"})
        world = world[["Country", "Continent", "geometry"]]

        # Spatial join: assign country/continent by location
        joined = gpd.sjoin(g_sites, world, predicate="within", how="left")

        # Handle missing matches using "intersects"
        na_mask = joined["Country"].isna()
        if na_mask.any():
            joined2 = gpd.sjoin(g_sites.loc[na_mask], world, predicate="intersects", how="left")
            joined.loc[na_mask, ["Country","Continent"]] = joined2[["Country","Continent"]].values

        # Add columns to dimension
        dim_site["Country"] = joined["Country"].values
        dim_site["Continent"] = joined["Continent"].values

    except Exception as e:
        print(f"Geographic enrichment skipped: {e}")  
        # Example print: "Geographic enrichment skipped: GeoPandas not available"
        dim_site["Country"] = np.nan
        dim_site["Continent"] = np.nan

    # Final column order
    cols_order = ["id_site", "AERONET_Site", "Latitude", "Longitude", "Elevation", "Country", "Continent"]
    dim_site = dim_site[[c for c in cols_order if c in dim_site.columns]]
    return dim_site

# =============================
# Main transform function
# =============================
def transform_aerosoles(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    # 1) Replace missing values (-999 codes → NaN)
    df.replace([-999, -999.0, "-999", "-999.0"], np.nan, inplace=True)

    # 2) Convert Date column if available
    if "Date(dd:mm:yyyy)" in df.columns:
        df["Date"] = pd.to_datetime(df["Date(dd:mm:yyyy)"], format="%d:%m:%Y", errors="coerce")

    # 3) Ensure numeric columns
    maybe_numeric = [
        "AOD_340nm","AOD_380nm","AOD_400nm","AOD_440nm","AOD_443nm","AOD_490nm","AOD_500nm",
        "AOD_510nm","AOD_532nm","AOD_551nm","AOD_555nm","AOD_560nm","AOD_620nm","AOD_667nm",
        "AOD_675nm","AOD_681nm","AOD_709nm","AOD_779nm","AOD_865nm","AOD_870nm","AOD_1020nm","AOD_1640nm",
        "Precipitable_Water(cm)","440-870_Angstrom_Exponent",
        "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"
    ]
    for col in maybe_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4) Add particle type classification
    ae_col = "440-870_Angstrom_Exponent"
    if ae_col in df.columns:
        df["Particle_type"] = df[ae_col].apply(_classify_particle_type)
    else:
        df["Particle_type"] = np.nan

    # 5) Reshape wide AOD columns into long format
    aod_cols = [c for c in df.columns if re.fullmatch(r"AOD_\d+nm", c)]
    if not aod_cols:
        raise ValueError("No AOD_*nm columns found in the DataFrame.")

    id_vars = [col for col in [
        "AERONET_Site", "Date", "Day_of_Year", "Precipitable_Water(cm)", ae_col, "Particle_type",
        "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"]
        if col in df.columns]

    df_long = df.melt(
        id_vars=id_vars,
        value_vars=aod_cols,
        var_name="AOD_band",
        value_name="AOD_Value"
    )

    # 6) Extract wavelength from column names
    df_long["Wavelength_nm"] = df_long["AOD_band"].str.extract(r"AOD_(\d+)nm").astype(float)

    # 7) Assign spectral band and aerosol sensitivity
    df_long["Spectral_Band"] = df_long["Wavelength_nm"].apply(_spectral_band)
    df_long["Sensitive_Aerosol"] = df_long["Wavelength_nm"].apply(_sensitive_aerosol)

    # 8) Drop rows with missing values
    df_long["AOD_Value"] = pd.to_numeric(df_long["AOD_Value"], errors="coerce")
    df_long = df_long.dropna(subset=["AOD_Value", "Wavelength_nm"]).reset_index(drop=True)

    # ============================
    # Dimension tables
    # ============================
    # Wavelength dimension
    dim_wavelength = (
        df_long[["Wavelength_nm", "Spectral_Band", "Sensitive_Aerosol"]]
        .drop_duplicates().sort_values("Wavelength_nm").reset_index(drop=True)
    )
    dim_wavelength["id_wavelength"] = dim_wavelength.index + 1

    # Date dimension
    dim_date = df_long[["Date"]].drop_duplicates().sort_values("Date").reset_index(drop=True)
    dim_date["id_date"] = dim_date.index + 1
    dim_date["Year"] = dim_date["Date"].dt.year.astype("int32")
    dim_date["Month"] = dim_date["Date"].dt.month.astype("int32")
    dim_date["Day"] = dim_date["Date"].dt.day.astype("int32")
    dim_date["Day_of_Year"] = dim_date["Date"].dt.dayofyear.astype("int32")

    # Site dimension (with country/continent enrichment)
    dim_site = _build_dim_site_with_country_continent(df_long)

    # ============================
    # Fact table
    # ============================
    fact_df = df_long.merge(dim_date[['id_date','Date']], on='Date', how='left')
    fact_df = fact_df.merge(dim_wavelength[['id_wavelength','Wavelength_nm']], on='Wavelength_nm', how='left')
    fact_df = fact_df.merge(dim_site[['id_site','AERONET_Site']], on='AERONET_Site', how='left')

    # Rename columns for Data Warehouse
    fact_df.rename(columns={
        'Precipitable_Water(cm)': 'Precipitable_Water',
        '440-870_Angstrom_Exponent': 'Angstrom_Exponent'
    }, inplace=True)

    # Add Fact ID
    fact_df = fact_df.reset_index(drop=True)
    fact_df["Fact_ID"] = fact_df.index + 1

    # Final column selection
    fact_df = fact_df[[
        "Fact_ID", "id_date", "id_wavelength", "id_site",
        "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent"
    ]]

    return fact_df, dim_wavelength, dim_date, dim_site