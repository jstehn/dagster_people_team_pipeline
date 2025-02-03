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

from ..utils import currency_to_decimal
from ..utils.constants import PAYCOM_REPORT_FILE_TEMPLATE

PAYCOM_DTYPES = {
    "Hire_Date": "datetime64[ns]",
    "Rehire_Date": "datetime64[ns]",
    "Termination_Date": "datetime64[ns]",
    "Salary": "currency",
    "Rate_1": "currency",
    "Annual_Salary": "currency",
}


@asset
def paycom_report_file(context: AssetExecutionContext) -> Output:
    """A point-in-time report of paycom data on employees."""
    context.log.warning("TODO: Implement this")
    return Output("people_team_data/data/raw/paycom/paycom.csv")

@asset(required_resource_keys={"db_resource"}, deps=[AssetKey("paycom_report_file")])
def paycom_report(
    context: AssetExecutionContext, paycom_report_file: str
) -> None:
    """Data that paycom has on employees."""
    paycom_data = pd.read_csv(paycom_report_file, dtype=str)
    context.log.debug(f"Loaded Paycom file. Current columns\n{paycom_data.dtypes}")
    for column, dtype in PAYCOM_DTYPES.items():
        if dtype == "currency":
            paycom_data[column] = pd.to_numeric(paycom_data[column].apply(currency_to_decimal), errors='coerce')
        elif dtype == "datetime64[ns]":
            paycom_data[column] = pd.to_datetime(paycom_data[column], errors="coerce")
        else:
            paycom_data[column] = paycom_data[column].where(
                paycom_data[column].isna(),
                paycom_data[column].astype(dtype)
            )

    # Ensure Employee_Code is a string type
    if 'Employee_Code' in paycom_data.columns:
        paycom_data['Employee_Code'] = paycom_data['Employee_Code'].astype(str)

    Session = context.resources.db_resource
    with Session() as session:
        paycom_data.to_sql(
            name='paycom_report',
            con=session.bind,
            if_exists='replace',
            index=False,
            method='multi'
        )
        session.commit()
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
