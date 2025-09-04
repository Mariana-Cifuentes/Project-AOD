# ETL Project – AERONET AOD

## Description
This project implements an ETL (Extract–Transform–Load) pipeline for an AERONET dataset containing daily averages of Aerosol Optical Depth (AOD) across multiple wavelengths. The solution migrates the data from a CSV into a MySQL Data Warehouse modeled with a star schema and prepares a visualization layer for further analysis.

**Objective:** Analyze the temporal, spatial, and spectral evolution of AOD to identify differences in the composition of atmospheric particles (fine vs. coarse) at AERONET observation sites operated by NASA.

---

## Problem Context
Atmospheric aerosols are tiny particles suspended in the air, originating from natural sources (dust, volcanic ash, sea salt, wildfires) and human activity (industrial pollution, traffic, agriculture). Although invisible, they significantly affect health, climate, and visibility: they deteriorate air quality, increase respiratory diseases, and alter solar radiation and rainfall patterns.

NASA’s AERONET network measures Aerosol Optical Depth (AOD) at different wavelengths. This not only indicates the concentration of particles in the atmosphere but also their predominant size: fine (smoke, pollution) or coarse (dust, sea salt). However, the data comes in massive, unwieldy files, with multiple columns, placeholder codes for missing values, and no clear geographic integration.

To tackle this challenge, the project implements an ETL pipeline that cleans, transforms, and enriches the dataset, then organizes it into a star schema within a Data Warehouse. This enables analysis of temporal and seasonal aerosol dynamics, spatial comparisons across regions, and spectral behavior exploration—turning raw data into actionable insights for climate science and environmental decision-making.

**CSV Source:** [AERONET Data](https://aeronet.gsfc.nasa.gov/new_web/download_all_v3_aod.html)

---

## General Flow

<img width="1021" height="472" alt="image" src="https://github.com/user-attachments/assets/5d63db16-2775-4d9c-b599-9d78c6bb33dd" />

---

## Tech Stack
- Python  
- MySQL  
- Jupyter Notebook (EDA)  
- Matplotlib (Visualization)  
- Git + GitHub  

---

## Repository Structure
````

PROYECTOETL/
├─ data/
│  ├─ All_Sites_Times_Daily_Averages_AOD20.csv
│  ├─ ne_50m_admin_0_countries.*     # Natural Earth shapefile
├─ eda.ipynb                         # EDA notebook
├─ extract.py                        # CSV extraction
├─ transform.py                      # Cleaning + dimensional modeling
├─ load.py                           # MySQL schema + insert
├─ main.py                           # ETL orchestrator
├─ Visualizations.ipynb              # Visualizations
├─ requirements.txt                  # Project dependencies
└─ venv/                             # Virtual environment


````

---

## EDA Summary (Raw File)
The original AERONET file contains over 1.3 million records with daily AOD measurements across multiple wavelengths. Key findings:
- Measurements standardized to daily timestamp (12:00:00 UTC).  
- Data covers numerous AERONET sites identified by latitude, longitude, and elevation.  
- Placeholder values of `-999` represent missing data.  
- Multiple AOD columns (`AOD_340nm … AOD_779nm`), requiring normalization.  
- Ångström Exponent (440–870) available for particle size classification.  
- Measurement counts vary across wavelengths and sites, justifying the star schema.  

**Conclusion:** The dataset is large, rich, and heterogeneous, but its raw format makes direct analysis impractical—supporting the need for the ETL pipeline.

---

## Star Schema Dimensional Model
At the core is a fact table (quantitative measurements) linked to several dimension tables (context).

<img width="748" height="767" alt="image" src="https://github.com/user-attachments/assets/6e3989bf-8352-4cd1-98af-37ff0089f4ca" />


### Fact Table: `Fact_AOD`
- **Fact_ID (PK):** Unique record ID  
- **Date_ID (FK → Dim_Date)**  
- **Site_ID (FK → Dim_Site)**  
- **Wavelength_ID (FK → Dim_Wavelength)**  
- **ParticleType_ID**  
- **AOD_Value (float)**  
- **Precipitable_Water (float, cm)**  
- **Angstrom_Exponent (float)**  

### Dimensions
**Dim_Date**  
- Date_ID, Date, Year, Month, Day, Day_of_Year  

**Dim_Site**  
- Site_ID, Site_Name, Latitude, Longitude, Elevation, Region  

**Dim_Wavelength**  
- Wavelength_ID, Wavelength_nm, Spectral_Band, Sensitive_Aerosol  

---

## Transformation Logic
The transform.py module prepares raw AERONET data into dimensional format for the Data Warehouse:

### Cleaning
- Replace sentinel values (-999) with NaN  
- Convert date column to datetime  
- Cast AOD, precipitable water, Ångström exponent, and coordinates to numeric  

### Particle Classification
Based on 440–870 Ångström Exponent:
- AE ≥ 1.5 → fine  
- AE ≤ 1.0 → coarse  
- In between → mixed  

### Spectral Normalization
- Use melt to reshape AOD_*nm columns from wide to long  
- Extract Wavelength_nm  
- Add Spectral_Band and Sensitive_Aerosol fields  

### Geographic Enrichment
- Build Dim_Site with unique sites, coordinates, and elevation  
- Validate/correct invalid coordinates  
- Assign Site_ID  
- If GeoPandas available: enrich with country and continent from Natural Earth shapefiles  

### Dimension Creation
- Dim_Wavelength, Dim_Date, Dim_Site  

### Fact Table
- Combine dimensions with long-format data into Fact_AOD  
- Standardize column names  
- Add Fact_ID  

### Output
`transform_aerosoles(df)` returns:
- fact_df  
- dim_wavelength  
- dim_date  
- dim_site  
 

---

## Prerequisites
```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
````

**Main dependencies:**

* pandas, numpy
* mysql-connector-python
* geopandas (optional)
* requests, zipfile

---

## MySQL Configuration

Environment variables:

| Variable        | Default     |
| --------------- | ----------- |
| MYSQL\_HOST     | localhost   |
| MYSQL\_PORT     | 3306        |
| MYSQL\_USER     | root        |
| MYSQL\_PASSWORD | root        |
| MYSQL\_DB       | aerosol\_dw |

**Create schema before running:**

```sql
CREATE DATABASE aerosol_dw;
```

---

## Running the ETL

1. Place CSV file in `./data`
2. Configure environment variables if different from defaults
3. Run:

```bash
python main.py
```

**Execution flow:** Extract → Transform → Load into MySQL

---

## Visualizations
To better understand aerosol behavior, four key visualizations were generated from the Data Warehouse:

### Temporal Evolution of AOD

<img width="864" height="556" alt="image" src="https://github.com/user-attachments/assets/6b9246f7-3dbc-4594-a9de-6e84c67ff04b" />

- Shows long-term AOD trends.  
- Low values (<0.1) = clear skies; high values (>1.0) = heavy particle load.  
- A notable decline occurs after the mid-1990s.  

### AOD Distribution by Continent

<img width="865" height="687" alt="image" src="https://github.com/user-attachments/assets/c2301600-675b-4e2c-9934-4198b612a588" />

- Boxplots highlight spatial variability.  
- Outliers correspond to extreme events (Saharan dust, fires, intense pollution), not measurement errors.  

### AOD vs. Ångström Exponent (AE)

<img width="850" height="559" alt="image" src="https://github.com/user-attachments/assets/0b95ab44-c423-405d-9964-7d6cff5d91f3" />

- Relates aerosol quantity (AOD) to particle size (AE).  
- High AOD often pairs with high AE, indicating fine particles during pollution or smoke episodes.  

### AE by Spectral Sensitivity Categories

<img width="858" height="559" alt="image" src="https://github.com/user-attachments/assets/cbf38607-a87d-4f89-a18e-14d832cfaa4c" />

- Compares fine-sensitive, balanced, and coarse-sensitive aerosols.  
- Fine particles show higher AE at shorter wavelengths; coarse ones remain low and stable.  

## Conclusion

This ETL pipeline transforms raw, unwieldy AERONET data into a **clean, dimensional model** ready for analysis. It enables climate and environmental researchers to study **temporal, spatial, and spectral dynamics of aerosols**.
