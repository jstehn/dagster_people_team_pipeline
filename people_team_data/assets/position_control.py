import os
from datetime import datetime

import gspread
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    EnvVar,
    MetadataValue,
    Output,
    asset,
)
from google.oauth2.service_account import Credentials

from people_team_data.resources.db_resource import DuckDBResource

from ..utils import currency_to_decimal
from ..utils.constants import POSITION_CONTROL_SHEETS_DIR


def download_all_sheets(credentials: Credentials, sheet_id: str, output_dir: str) -> list[str]:
    """Downloads all sheets from a Google Sheets document and saves them as CSV files."""
    client = gspread.authorize(credentials)
    sheet = client.open_by_key(sheet_id)
    worksheets = sheet.worksheets()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    file_paths = []
    for worksheet in worksheets:
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        subfolder = os.path.join(output_dir, datetime.now().strftime('%Y-%m-%d'))
        if not os.path.exists(subfolder):
            os.makedirs(subfolder, exist_ok=True)
        file_path = os.path.join(subfolder, f"{worksheet.title}.csv")
        df.to_csv(file_path, index=False)
        file_paths.append(file_path)
    
    return file_paths

@asset(required_resource_keys={"google_service_account"})
def position_control_sheets(context: AssetExecutionContext) -> Output[list[str]]:
    """Downloads all sheets from the Position Control Google Sheets document and saves them as CSV files."""
    credentials: Credentials = context.resources.google_service_account
    sheet_id = EnvVar("POSITION_CONTROL_SHEET_ID").get_value()
    output_dir = POSITION_CONTROL_SHEETS_DIR

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    file_paths = download_all_sheets(credentials, sheet_id, output_dir)

    context.log.info(f"Downloaded all sheets from Google Sheets document '{sheet_id}' and saved them to '{output_dir}'")

    metadata = {
        "sheet_id": MetadataValue.text(sheet_id),
        "output_dir": MetadataValue.path(output_dir),
        "file_paths": MetadataValue.json(file_paths),
    }

    return Output(value=file_paths, metadata=metadata)

def create_position_control_csv_asset(sheet_name: str, config: dict = None):
    config = config or {}
    deps = ["position_control_sheets"] + [field.split(".")[0] for field in config.get("foreign_keys", {}).values()]
    
    @asset(name=f"position_control_{sheet_name.lower()}", deps=[AssetKey(dep) for dep in deps])
    def position_control_asset(context: AssetExecutionContext, database: DuckDBResource, position_control_sheets: list[str]) -> Output[str]:
        """Loads the contents of a specific CSV file for a sheet and uploads it to the DuckDB database."""
        file_path = next((path for path in position_control_sheets if sheet_name in path), None)
        if not file_path:
            raise FileNotFoundError(f"CSV file for sheet '{sheet_name}' not found in downloaded files.")

        df = pd.read_csv(file_path)

        # Apply the configuration to the DataFrame
        if config:
            # Convert columns to the specified types
            for column, dtype in config.get("columns", {}).items():
                if dtype == "currency":
                    df[column] = pd.to_numeric(df[column].apply(currency_to_decimal), errors='coerce', downcast='float')
                elif dtype == "datetime64[ns]":
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif dtype == "int":
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
                elif dtype == "float":
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
                else:
                    df[column] = df[column].astype(dtype, errors='ignore')

            # Drop rows with null values in non-null columns
            non_null_columns = config.get("non_null", [])
            df.dropna(subset=non_null_columns, inplace=True)

        context.log.info(f"Loaded CSV file '{file_path}' with {df.shape[0]} rows and {df.shape[1]} columns")
        df.dropna(how='all', inplace=True)
        with database.get_connection() as conn:
            df.to_sql(
                name=f'position_control_{sheet_name.lower()}',
                con=conn,
                if_exists='replace',
                index=False,
                method='multi'
            )

        metadata = {
            "file_path": MetadataValue.path(file_path),
            "num_rows": MetadataValue.int(df.shape[0]),
            "num_columns": MetadataValue.int(df.shape[1]),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }

        return Output(value=f'position_control_{sheet_name.lower()}', metadata=metadata)

    return position_control_asset

# Create assets for each sheet
positions_config = {
    "columns": {
        "Position_ID": "str",
        "Position_Unique":  "bool",
        "Assignment_Count": "int",
        "Position_Start": "datetime64[ns]",
        "Position_End": "datetime64[ns]",
        "Position_Status": "str",
        "Position_HRDepartment": "str",
        "Position_HRDivision": "str",
        "Position_HRSubDepartment": "str",
        "Position_Code": "str",
        "Position_Count": "str",
        "Position_Name": "str",
        "Position_Account": "str",
        "Position_Goal": "str",
        "Position_Full": "str",
        "Position_Credential": "str",
        "Position_Supervisor": "str",
        "Position_Represented": "str",
        "Notes": "str",
    },
    "primary_key": "Position ID",
    "non_null": ["Position_ID"]
}
position_control_positions = create_position_control_csv_asset("Positions", config=positions_config)

employees_config = {
    "columns": {
        "Employee_ID": "str",
        "Employee_InitialHireDate": "datetime64[ns]",
        "Employee_RehireDate": "datetime64[ns]",
        "Employee_EndDate": "datetime64[ns]",
        "Employee_Type": "str",
        "Employee_Status": "str",
        "Employee_FirstName": "str",
        "Employee_MiddleName": "str",
        "Employee_LastName": "str",
        "Employee_Email": "str",
        "Employee_Phone": "str",
        "Employee_YearsOfServiceStart": "str",
        "Employee_FullName": "str",
        "Employee_Full": "str",
        "Employee_SEID": "str",
        "Employee_Gender": "str",
        "Employee_Race": "str",
        "Employee_Hispanic": "str",
        "Employee_CALPADSStartYearCertificated": "str",
        "Employee_CALPADSCaliberStartYearCertificated": "str",
        "Notes": "str",
    },
    "primary_key": "Employee_ID",
    "non_null": ["Employee_ID"]
}
position_control_employees = create_position_control_csv_asset("Employees", config=employees_config)


adjustments_config = {
    "columns": {
        "Adjustment_ID": "str", # Meant to be ID but is currently blank
        "Assignment_Full": "str",
        "Adjustment_Status": "str",
        "Adjustment_Category": "str",
        "Adjustment_Description": "str",
        "Adjustment_Salary": "currency",
        "Adjustment_Hourly": "currency",
        "Adjustment_Begin_Payroll": "datetime64[ns]",
        "Adjustment_End_Payroll": "datetime64[ns]",
        "Adjustment_#": "str",
        "Adjustment_PPP": "datetime64[ns]",
        "Notes": "str",
    },
    "non_null": ["Assignment_Full", "Adjustment_Status"],
    "foreign_keys": {
        "Assignment_Full": "position_control_assignments.Assignment_Full"
    }
}
position_control_adjustments = create_position_control_csv_asset("Adjustments", config=adjustments_config)

stipends_config = {
    "columns": {
        "Stipend_ID": "str",
        "Employee_ID": "str",
        "Employee_Name": "str",
        "Stipend_Start_Date": "datetime64[ns]",
        "Stipend_End_Date": "datetime64[ns]",
        "Stipend_Status": "str",
        "Stipend_Category": "str",
        "Stipend_Description": "str",
        "Stipend_Amount": "currency",
        "Stipend_FullAccount": "str",
        "Stipend_Account": "str",
        "Stipend_FullGoal": "str",
        "Stipend_Goal": "str",
        "Stipend_Location": "str",
        "Stipend_FullResource": "str",
        "Stipend_Resource": "str",
        "Notes": "str",
    },
    "non_null": ["Stipend_ID", "Employee_ID", "Employee_Name"],
    "primary_key": "Stipend_ID",
    "foreign_keys": {
        "Employee_ID": "position_control_employees.Employee_ID"
    }
}
position_control_stipends = create_position_control_csv_asset("Stipends", config=stipends_config)

assignments_config = {
    "columns": {
        "Assignment_ID": "str",
        "Assignment_Start": "datetime64[ns]",
        "Assignment_End": "datetime64[ns]",
        "Assignment_Count": "int",
        "Assignment_Status": "str",
        "Position_ID": "str",
        "Position_Full": "str",
        "Employee_ID": "str",
        "Employee_Count": "int",
        "Employee_Full": "str",
        "Assignment_Full": "str",
        "Assignment_FTE": "float",
        "Assignment_Calendar": "int",
        "Assignment_Scale": "str",
        "Assignment_Step": "int",
        "Employee_YearsOfServiceStart": "str",
        "Assignment_Column": "str",
        "Assignment_Salary": "currency",
        "Assignment_Wage": "currency",
        "Assignment_PPP": "currency",
        "Position_Account": "str",
        "Position_Goal": "str",
        "Assignment_Resource": "str",
        "Assignment_FullResource": "str",
        "Notes": "str",
    },
    "primary_key": "Assignment_ID",
    "non_null": ["Assignment_ID"],
    "foreign_keys": {
        "Position_ID": "position_control_positions.Position_ID",
        "Employee_ID": "position_control_employees.Employee_ID"
    }
}
position_control_assignments = create_position_control_csv_asset("Assignments", config=assignments_config)
