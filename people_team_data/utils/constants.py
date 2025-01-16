DATA_DIR = "people_team_data/data/"
DATA_RAW = DATA_DIR + "raw/"
DATA_PROCESSED = DATA_DIR + "processed/"
DATA_DEV = DATA_DIR + "dev/"
DUCKDB_LOCATION = DATA_DEV + "duckdb.db"

BAMBOOHR_REPORT_DIR = DATA_RAW + "bamboohr/"
BAMBOOHR_REPORT_FILE_TEMPLATE = BAMBOOHR_REPORT_DIR + "report-{date}.json"
PAYCOM_REPORT_DIR = DATA_RAW + "paycom/"
PAYCOM_REPORT_FILE_TEMPLATE = PAYCOM_REPORT_DIR + "report-{date}.json"

START_DATE = "2021-01-01" # TODO: Change these for later.
END_DATE = "2021-12-31"