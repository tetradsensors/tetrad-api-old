from os import getenv 


# Get env variables
PROJECT_ID = getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET_TELEMETRY = getenv("BQ_DATASET_TELEMETRY") 
BQ_TABLE_SLC    = getenv("BQ_TABLE_SLC")
BQ_TABLE_CHATT  = getenv("BQ_TABLE_CHATT")
BQ_TABLE_CLEV   = getenv("BQ_TABLE_CLEV")
BQ_TABLE_KC     = getenv("BQ_TABLE_KC")
BQ_TABLE_BADGPS = getenv("BQ_TABLE_BADGPS")
BQ_TABLE_GLOBAL = getenv("BQ_TABLE_GLOBAL")

SPACE_KERNEL_FACTOR_PADDING = float(getenv("SPACE_KERNEL_FACTOR_PADDING"))
TIME_KERNEL_FACTOR_PADDING = float(getenv("TIME_KERNEL_FACTOR_PADDING"))

# Table sources as they appear in route arguments
# (the keys) and their mapping to BigQuery table names
SRC_MAP = {
    "SLC":    BQ_TABLE_SLC,
    "CHATT":  BQ_TABLE_CHATT,
    "CLEV":   BQ_TABLE_CLEV,
    "KC":     BQ_TABLE_KC,
    "BADGPS": BQ_TABLE_BADGPS,
    "GLOBAL": BQ_TABLE_GLOBAL,
    "ALL":    None,
    "ALLGPS": None,
}

ALLGPS_TBLS = {
    "SLC":    BQ_TABLE_SLC,
    "CHATT":  BQ_TABLE_CHATT,
    "CLEV":   BQ_TABLE_CLEV,
    "KC":     BQ_TABLE_KC,
    "GLOBAL": BQ_TABLE_GLOBAL,
}

ELEV_MAPS = {
    "SLC":   getenv("ELEV_MAP_SLC_FILENAME"),
    "CHATT": getenv("ELEV_MAP_CHATT_FILENAME"),
    "CLEV":  getenv("ELEV_MAP_CLEV_FILENAME"),
    "KC":    getenv("ELEV_MAP_KC_FILENAME"),
}

# Field names as they appear in route arguments
#   and their mapping to BigQuery field names
FIELD_MAP = {
    "TIMESTAMP":    getenv("FIELD_TS"),
    "DEVICEID":     getenv("FIELD_ID"),
    "LATITUDE":     getenv("FIELD_LAT"),
    "LONGITUDE":    getenv("FIELD_LON"),
    "ELEVATION":    getenv("FIELD_ELE"),
    "PM1":          getenv("FIELD_PM1"),
    "PM2_5":        getenv("FIELD_PM2"),
    "PM10":         getenv("FIELD_PM10"),
    "TEMPERATURE":  getenv("FIELD_TEMP"),
    "HUMIDITY":     getenv("FIELD_HUM"),
    "MICSRED":      getenv("FIELD_RED"),
    "MICSNOX":      getenv("FIELD_NOX"),
    "MICSHEATER":   getenv("FIELD_HTR"),
}

# The query-able fields
VALID_QUERY_FIELDS = {
    "ELEVATION":    getenv("FIELD_ELE"),
    "PM1":          getenv("FIELD_PM1"),
    "PM2_5":        getenv("FIELD_PM2"),
    "PM10":         getenv("FIELD_PM10"),
    "TEMPERATURE":  getenv("FIELD_TEMP"),
    "HUMIDITY":     getenv("FIELD_HUM"),
    "MICSRED":      getenv("FIELD_RED"),
    "MICSNOX":      getenv("FIELD_NOX"),
    "MICSHEATER":   getenv("FIELD_HTR"),
}