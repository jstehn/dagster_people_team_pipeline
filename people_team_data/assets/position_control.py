"""Assets related to Position Control, which is a Google Sheet."""

import gspread
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, Output, asset
from google.oauth2.service_account import Credentials


@asset(required_resource_keys={"google_service_account"})
def position_control_sheet(context: AssetExecutionContext) -> Output:
    """Loads the contents of the Position Control worksheet from Google Sheets."""
    # Get the service account credentials from the resource
    credentials: Credentials = context.resources.google_service_account

    # Authorize the client
    client = gspread.authorize(credentials)

    # Open the Google Sheets document by ID
    sheet_id = context.resources.google_service_account.sheet_id
    sheet = client.open_by_key(sheet_id)

    # Select the worksheet by name
    worksheet_name = context.resources.google_service_account.worksheet_name
    worksheet = sheet.worksheet(worksheet_name)

    # Get all values from the worksheet
    data = worksheet.get_all_values()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    # Log the number of rows and columns
    context.log.info(f"Loaded worksheet '{worksheet_name}' with {df.shape[0]} rows and {df.shape[1]} columns")

    # Prepare metadata for the Output
    metadata = {
        "sheet_id": MetadataValue.text(sheet_id),
        "worksheet_name": MetadataValue.text(worksheet_name),
        "num_rows": MetadataValue.int(df.shape[0]),
        "num_columns": MetadataValue.int(df.shape[1]),
        "preview": MetadataValue.md(df.head().to_markdown()),
    }

    return Output(value=df, metadata=metadata)