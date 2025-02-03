import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset


@asset(required_resource_keys={"db_resource"}, deps=["paycom_report", "bamboohr_report"])
def employee_discrepancy_report(
    context: AssetExecutionContext
) -> MaterializeResult:
    """A report of all discrepancies between source information."""
    return #TODO: Implement
    # Query the paycom_report and bamboohr_report tables from the database
    with database.get_connection() as conn:
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

    # Save the discrepancies to a new table in the database
    discrepancies_table = "employee_discrepancies"
    with database.get_connection() as conn:
        discrepancies.to_sql(discrepancies_table, conn, if_exists="replace")

    # Prepare metadata for the MaterializeResult
    metadata = {
        "num_discrepancies": MetadataValue.int(discrepancies.shape[0]),
        "discrepancies_table": MetadataValue.text(discrepancies_table),
    }

    return MaterializeResult(metadata=metadata)