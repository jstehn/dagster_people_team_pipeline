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
def position_control_sheets(context: AssetExecutionContext) -> Output:
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

def create_position_control_csv_asset(sheet_name: str):
    @asset(name=f"position_control_{sheet_name.lower()}", deps=[AssetKey("position_control_sheets")])
    def position_control_asset(context: AssetExecutionContext, database: DuckDBResource, position_control_sheets: list[str]) -> None:
        """Loads the contents of a specific CSV file for a sheet and uploads it to the DuckDB database."""
        file_path = next((path for path in position_control_sheets if sheet_name in path), None)
        if not file_path:
            raise FileNotFoundError(f"CSV file for sheet '{sheet_name}' not found in downloaded files.")

        df = pd.read_csv(file_path)

        context.log.info(f"Loaded CSV file '{file_path}' with {df.shape[0]} rows and {df.shape[1]} columns")

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

        return Output(value=df, metadata=metadata)

    return position_control_asset

# Create assets for each sheet
position_control_positions = create_position_control_csv_asset("Positions")
position_control_employees = create_position_control_csv_asset("Employees")
position_control_assignments = create_position_control_csv_asset("Assignments")
position_control_adjustments = create_position_control_csv_asset("Adjustments")
position_control_stipends = create_position_control_csv_asset("Stipends")