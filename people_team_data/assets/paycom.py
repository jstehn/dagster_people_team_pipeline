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

from people_team_data.resources.db_resource import DuckDBResource

from ..utils import currency_to_decimal
from ..utils.constants import PAYCOM_REPORT_FILE_TEMPLATE

PAYCOM_DTYPES = {
    "Hire_Date": "time",
	"Rehire_Date": "time",
	"Termination_Date": "time",
	"Salary": "currency",
	"Rate_1": "currency",
	"Annual_Salary": "currency",
	"Employee_Code": int
}


@asset
def paycom_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of paycom data on employees."""
    context.log.warning("TODO: Implement this")
    return Output("people_team_data/data/raw/paycom/paycom.csv")

@asset(deps=[AssetKey("paycom_report_file")])
def paycom_report(
    context: AssetExecutionContext, database: DuckDBResource, paycom_report_file: str
) -> None:
    """Data that paycom has on employees."""
    paycom_data = pd.read_csv(paycom_report_file, dtype=str)
    context.log.debug(f"Loaded Paycom file. Current columns\n{paycom_data.dtypes}")
    for column, dtype in PAYCOM_DTYPES.items():
        if dtype == "currency":
            paycom_data[column] = pd.to_numeric(paycom_data[column].apply(currency_to_decimal))
        elif dtype == "time":
            paycom_data[column] = pd.to_datetime(paycom_data[column], errors="coerce")
        else:
            paycom_data[column] = paycom_data[column].where(
                paycom_data[column].isna(),
                paycom_data[column].astype(dtype)
            )
    with database.get_connection() as conn:
        paycom_data.to_sql(
            name='paycom_report',
            con=conn,
            if_exists='replace',
            index=False,
            method='multi',
            dtype={"Employee_Code": "INTEGER PRIMARY KEY"}
        )
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
