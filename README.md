# Measuring the effects of tropical forest disturbance with GEDI
[![Hippocratic License HL3-ECO-EXTR-FFD-MIL-SV](https://img.shields.io/static/v1?label=Hippocratic%20License&message=HL3-ECO-EXTR-FFD-MIL-SV&labelColor=5e2751&color=bc8c3d)](https://firstdonoharm.dev/version/3/0/eco-extr-ffd-mil-sv.html)
 <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

## Requirements
- Python 3.8+
- Java 11.0+
- PostGIS (to find coincident footprints)

## Getting started

This repository contains the code required to reproduce the results and figures
for the paper "Repeat GEDI footprints enable the measurement of moderate-scale tropical forest disturbance."
This repository may continue to receive code updates and efficiency improvements,
but the version used for the paper is tagged with \<publication-freeze\>

### Data sources
1. GEDI Level 2A, Level 2B, and Level 4A.

    These data products are available from the ORNL and LP DAACs. This repository
    assumes that the data is already downloaded and filtered with the shots
    loaded into a PostGIS database. Please contact the corresponding authors
    if assistance is needed with this step. Alternatively, if given an existing
    dataset of coincident shots, the later pipelines can be run without PostGIS
    by supplying the argument <p>`--shots_dir=/path/to/coincident/shots/dataset`</p>

2. JRC Annual Forest Change maps and intensity data ("v1_2022")

    These rasters are available from https://forobs.jrc.ec.europa.eu/ and on
    Google Earth Engine. The codebase assumes that this data is already downloaded
    and saved as aligned raster files. File naming conventions are enforced in
    `data/jrc_intensity_parser.py` and `data/jrc_parser.py`; these conventions match
    the file naming if the Annual Forest Change maps are downloaded from the
    JRC website and the intensity metrics are downloaded from Google Earth Engine.
    However, you may need to adjust these regular expressions to match your files.

3. GLAD disturbance maps ("v20231206")

    These rasters are available from https://glad.umd.edu/dataset/glad-forest-alerts.
    They must be preprocessed to reformat the data and remove plantation forests
    according to the basemap available from 
    https://console.cloud.google.com/storage/browser/earthenginepartners-hansen/S2alert/forestMask.
    This step can be performed using the script in `data/glad_preprocessor.py`.

### Setup
In addition to setting up the data as described above, set up the environment as follows:
Create a file called '.env' in the main repo directory with the following structure:
```python
# User path constants
USER_PATH="/home/<user>"
DATA_PATH="/path/to/data"
RESULTS_PATH="/path/for/results"

# Raster data locations
# Specify data location as a subdirectory of the DATA_PATH given above.
# e.g. if the AFC data is saved in "/path/to/data/AFC/v1_2022",
# AFC_SUBDIR should be set to the string "AFC/v1_2022"
AFC_SUBDIR = "AFC/v1_2022"
GLAD_SUBDIR = "GLAD"
# Non-plantation mask for the GLAD data
GLAD_BASEMAP_SUBDIR = "GLADS2/forest_mask"

# GEDI database constants
DB_HOST="<hostname>"
DB_NAME="<db name>"
DB_USER="<db user>"
DB_PASSWORD="<db password>"
```
3. Install required python packages (requirements/requirements.txt). Install the local module with
```sh
pip install -e /path/to/repo
```

### Processing pipelines
This codebase contains four processing pipelines used to generate the data for
the paper.

1. find_coincident_shots.py: Find coincident shot pairs (< 40 m apart) for a given region.
2. jrc_degradation_pipeline.py: Identify coincident shot pairs that overlap with disturbance events in the AFC dataset.
3. glad_degradation_pipeline.py: Identify coincident shot pairs that overlap with disturbance events in the GLAD dataset.
4. jrc_disturb_intensity_pipeline.py: For a set of AFC-detected disturbance events, get the corresponding disturbance intensity.

Additionally, it includes a pipeline for identifying coincident shot pairs that fall in "intact" forest. This is only used in the supplementary analysis (not the main paper).

## Project Organization
```
├── LICENSE
├── README.md          <- The top-level README for developers using this project.
|
|── figures            <- Jupyter notebooks used to produce the figures that
|                           appear in the paper and supplementary material.
│
├── requirements       <- Directory containing the requirement files.
│
├── setup.py           <- makes project pip installable 
├── src                <- Source code for use in this project.
│   ├── __init__.py    <- Makes src a Python module
│   │
│   ├── data           <- Functions to load data and perform basic parsing
│   │
│   ├── processing     <- Functions to perform complex data processing/overlays
|   |
│   ├── pipelines      <- Scripts to run end-to-end processing pipelines
│   │
│   ├── utils          <- General-purpose utility functions
│   │
│   ├── tests          <- Unit tests of (some) functions
│   │
│   └── constants.py   <- Environment variables, path constants, etc.
│
└── setup.cfg          <- setup configuration file for linting rules
```

---------------------

Project template created by the [Cambridge AI4ER Cookiecutter](https://github.com/ai4er-cdt/ai4er-cookiecutter).
