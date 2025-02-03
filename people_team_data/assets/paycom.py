"""Assets related to importing data from Paycom"""
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
)
from dagster_duckdb import DuckDBResource

from ..config.asset_configs import paycom_config
from ..utils import apply_config_to_dataframe
from ..utils.constants import PAYCOM_REPORT_FILE_TEMPLATE


@asset
def paycom_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of paycom data on employees."""
    context.log.warning("TODO: Implement this")
    return Output("people_team_data/data/raw/paycom/paycom.csv")

@asset(deps=[AssetKey("paycom_report_file")])
def paycom_report(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    paycom_report_file: str
) -> None:
    """Data that paycom has on employees."""
    paycom_data = pd.read_csv(paycom_report_file, dtype=str)
    context.log.debug(f"Loaded Paycom file. Current columns\n{paycom_data.dtypes}")
    paycom_data = apply_config_to_dataframe(paycom_data, paycom_config)

    # Ensure Employee_Code is a string type
    if 'Employee_Code' in paycom_data.columns:
        paycom_data['Employee_Code'] = paycom_data['Employee_Code'].astype(str)

    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE paycom_report AS SELECT * FROM paycom_data")
        
    df_preview = paycom_data.head().to_markdown()
    metadata = {
        "num_rows": MetadataValue.int(paycom_data.shape[0]),
        "num_columns": MetadataValue.int(paycom_data.shape[1]),
        "columns": MetadataValue.json(
            {col: str(dtype) for col, dtype in paycom_data.dtypes.items()}
        ),
        "preview": MetadataValue.md(df_preview),
    }
    return MaterializeResult(metadata=metadata)
