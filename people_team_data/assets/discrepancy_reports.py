import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from ..resources import postgres_db

@asset(
    deps=["paycom_report", "bamboohr_report"],
    required_resource_keys={"postgres_db"},
    resource_defs={
        "postgres_db": postgres_db.configured({
            "dbname": "calibrate",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432,
        })
    },
)
def employee_discrepancy_report(context: AssetExecutionContext) -> MaterializeResult:
    """A report of all discrepancies between source information."""
    # Query the paycom_report and bamboohr_report tables from the database
    return
    with context.resources.postgres_db as conn:
        paycom_df = pd.read_sql("SELECT * FROM paycom_report", conn)
        bamboohr_df = pd.read_sql("SELECT * FROM bamboohr_report", conn)

    # Ensure both DataFrames have the same index for accurate comparison
    paycom_df.set_index("Employee_Code", inplace=True)
    bamboohr_df.set_index("employeeNumber", inplace=True)

    # Align the columns of both DataFrames
    common_columns = paycom_df.columns.intersection(bamboohr_df.columns)
    paycom_df = paycom_df[common_columns]
    bamboohr_df = bamboohr_df[common_columns]

    # Compare the DataFrames to find discrepancies
    discrepancies = paycom_df.compare(bamboohr_df, keep_equal=False)

    # Log the discrepancies
    context.log.info(f"Found discrepancies:\n{discrepancies}")

    return MaterializeResult(
        asset_key="employee_discrepancy_report",
        metadata={"discrepancies": MetadataValue.text(str(discrepancies))}
    )