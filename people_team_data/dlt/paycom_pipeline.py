import dlt
from dlt.sources.filesystem import filesystem, read_csv


# This resource creates a filesystem source that loads CSV files from your Google Drive folder.
# The incremental hint tracks files by "modification_date" so that previously processed files are skipped.
@dlt.resource(name="paycom_latest_csv", primary_key="Employee_Code")
def load_latest_csv(
    bucket_url: str = dlt.config.value, file_glob: str = dlt.config.value
):
    # Create the filesystem source using the provided bucket_url and file_glob.
    fs_source = filesystem(bucket_url=bucket_url, file_glob=file_glob)
    # Apply an incremental hint so only new or updated files are processed.
    fs_source.apply_hints(
        incremental=dlt.sources.incremental("modification_date")
    )
    filesystem_pipe = (fs_source | read_csv()).with_name("paycom_data")
    # Instead of calling a method like fs_source.read_csv(), use the pipe operator to chain the CSV transformer.
    return filesystem_pipe


# Group the resource under a source schema.
@dlt.source(name="paycom_source")
def paycom_source():
    return load_latest_csv()


def run_pipeline():
    # Create a pipeline that writes to PostgreSQL.
    # Make sure your .dlt/secrets.toml is correctly configured with your PostgreSQL connection details.
    pipeline = dlt.pipeline(
        pipeline_name="paycom_pipeline",
        destination="postgres",
        dataset_name="paycom_data",
    )
    # Run the pipeline; the loader_file_format can be specified if needed.
    load_info = pipeline.run(paycom_source())
    print(load_info)


if __name__ == "__main__":
    run_pipeline()
