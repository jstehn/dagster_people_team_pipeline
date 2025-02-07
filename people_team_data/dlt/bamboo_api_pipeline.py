import dlt
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.rest_api import EndpointResource, rest_api_source


@dlt.source
def bamboohr(
    api_key: str = dlt.secrets.value,
    company_domain: str = dlt.secrets.value,
    fields: list = dlt.secrets.value,
):
    config = {
        "client": {
            "base_url": f"https://api.bamboohr.com/api/gateway.php/{company_domain}/v1/",
            "auth": HttpBasicAuth(api_key, "x"),
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/json",  # Required for POST
            },
        },
        "resources": [
            EndpointResource(
                name="employee_dataset",
                endpoint={
                    "path": "datasets/employee",
                    "method": "POST",
                    "json": {"fields": fields},
                    "params": {"format": "JSON"},
                    "paginator": {
                        "type": "offset",
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "limit": 100,
                    },
                },
            )
        ],
    }

    return rest_api_source(config)


if __name__ == "__main__":
    # Initialize pipeline with filesystem destination for CSV
    pipeline = dlt.pipeline(
        pipeline_name="bamboo_csv",
        destination="filesystem",
        dataset_name="bamboo_data",
    )

    # Run the pipeline with CSV format
    load_info = pipeline.run(
        bamboohr(), loader_file_format="csv", write_disposition="replace"
    )

    # Print output location
    print(f"Data saved to: {pipeline.working_dir}")
