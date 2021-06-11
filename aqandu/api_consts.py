from os import getenv 


# Get env variables
PROJECT_ID = getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET_TELEMETRY = getenv("BQ_DATASET_TELEMETRY")
BQ_TABLE_TELEMETRY = getenv("BQ_TABLE_TELEMETRY")
BQ_PATH_TELEMETRY = f"{PROJECT_ID}.{BQ_DATASET_TELEMETRY}.{BQ_TABLE_TELEMETRY}"
# BQ_LABEL_SLC    = getenv("BQ_TABLE_SLC")
# BQ_LABEL_CHATT  = getenv("BQ_TABLE_CHATT")
# BQ_LABEL_CLEV   = getenv("BQ_TABLE_CLEV")
# BQ_TABLE_KC     = getenv("BQ_TABLE_KC")
BQ_LABEL_BADGPS = getenv("BQ_LABEL_BADGPS")
BQ_LABEL_GLOBAL = getenv("BQ_LABEL_BADGPS")

SPACE_KERNEL_FACTOR_PADDING = float(getenv("SPACE_KERNEL_FACTOR_PADDING"))
TIME_KERNEL_FACTOR_PADDING = float(getenv("TIME_KERNEL_FACTOR_PADDING"))

Q_ALL_SOURCES = "all"
Q_ALL_GPS_SOURCES = "allgps"

# Table sources as they appear in route arguments
# (the keys) and their mapping to BigQuery table names
# REGION_MAP = {
#     # getenv("Q_SLC"):    BQ_TABLE_SLC,
#     # getenv("Q_CHATT"):  BQ_TABLE_CHATT,
#     # getenv("Q_CLEV"):   BQ_TABLE_CLEV,
#     # getenv("Q_KC"):     BQ_TABLE_KC,
#     getenv("Q_BADGPS"): BQ_LABEL_BADGPS,
#     getenv("Q_GLOBAL"): BQ_LABEL_GLOBAL,
#     getenv("Q_ALL"):    None,
#     getenv("Q_ALLGPS"): None,
# }

# ALL_GPS_LABELS = {
#     getenv("Q_SLC"):    BQ_TABLE_SLC,
#     getenv("Q_CHATT"):  BQ_TABLE_CHATT,
#     getenv("Q_CLEV"):   BQ_TABLE_CLEV,
#     getenv("Q_KC"):     BQ_TABLE_KC,
#     getenv("Q_GLOBAL"): BQ_TABLE_GLOBAL,
# }

# ELEV_MAPS = {
#     getenv("Q_SLC"):   getenv("ELEV_MAP_SLC_FILENAME"),
#     getenv("Q_CHATT"): getenv("ELEV_MAP_CHATT_FILENAME"),
#     getenv("Q_CLEV"):  getenv("ELEV_MAP_CLEV_FILENAME"),
#     getenv("Q_KC"):    getenv("ELEV_MAP_KC_FILENAME"),
# }

# Field names as they appear in route arguments
#   and their mapping to BigQuery field names
FIELD_MAP = {
    "TIMESTAMP":    getenv("FIELD_TS"),
    "DEVICEID":     getenv("FIELD_ID"),
    "GPS":          getenv("FIELD_GPS"),
    "ELEVATION":    getenv("FIELD_ELE"),
    "PM1":          getenv("FIELD_PM1"),
    "PM2_5":        getenv("FIELD_PM2"),
    "PM10":         getenv("FIELD_PM10"),
    "TEMPERATURE":  getenv("FIELD_TEMP"),
    "HUMIDTY":      getenv("FIELD_HUM"),
    "MICSRED":      getenv("FIELD_RED"),
    "MICSNOX":      getenv("FIELD_NOX"),
    "MICSHEATER":   getenv("FIELD_HTR"),
    "RSSI":         getenv("FIELD_RSSI"),
    "PM_RAW":       getenv("FIELD_PMRAW"),
    "SOURCE":       getenv("FIELD_SRC"),
    "LABEL":        getenv("FIELD_LBL"),
}

# The query-able fields
VALID_QUERY_FIELDS = {
    "ELEVATION":    getenv("FIELD_ELE"),
    "PM1":          getenv("FIELD_PM1"),
    "PM2_5":        getenv("FIELD_PM2"),
    "PM10":         getenv("FIELD_PM10"),
    "PM_RAW":       getenv("FIELD_PMRAW"),
    "RSSI":         getenv("FIELD_RSSI"),
    "TEMPERATURE":  getenv("FIELD_TEMP"),
    "HUMIDITY":     getenv("FIELD_HUM"),
    "MICSRED":      getenv("FIELD_RED"),
    "MICSNOX":      getenv("FIELD_NOX"),
    "MICSHEATER":   getenv("FIELD_HTR"),
}