import dlt
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.rest_api.config_setup import register_paginator


class BambooHRPaginator(BasePaginator):
    def __init__(self, page_size=100):
        super().__init__()
        self.page = 1
        self.page_size = page_size
        self.total_pages = None

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}
        request.params.update({"page": self.page, "page_size": self.page_size})

    def update_state(self, response: Response, data=None) -> None:
        pagination = response.json().get("pagination", {})
        self.total_pages = pagination.get("total_pages", self.total_pages)
        if self.total_pages and self.page >= self.total_pages:
            self._has_next_page = False
        else:
            self.page += 1


register_paginator("BambooHRPaginator", BambooHRPaginator)


@dlt.source
def bamboohr_source(
    api_key: str = dlt.secrets.value,
    company_domain: str = dlt.secrets.value,
    fields: list = dlt.secrets.value,
):
    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://api.bamboohr.com/api/gateway.php/{company_domain}/v1/",
            "auth": HttpBasicAuth(api_key, "x"),
            "paginator": "BambooHRPaginator",
        },
        "resource_defaults": {
            "primary_key": "employeeNumber",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "raw_employee_data",
                "endpoint": {
                    "path": "datasets/employee",
                    "method": "POST",
                    "json": {
                        "fields": fields,
                    },
                },
            },
        ],
    }

    # Create the raw data resource
    raw_source = rest_api_source(config)

    @dlt.resource(name="processed_employee_data", primary_key="employeeNumber")
    def processed_employee_data(raw_employee_data):
        seen_employee_numbers = set()
        for record in raw_employee_data:
            employee_number = record.get("employeeNumber")
            if employee_number is not None:
                # Normalize employeeNumber to 4 characters with leading zeros
                employee_number = str(employee_number).zfill(4)
                if employee_number not in seen_employee_numbers:
                    seen_employee_numbers.add(employee_number)
                    record["employeeNumber"] = employee_number
                    yield record

    # Retrieve the raw data resource
    raw_data = raw_source.resources["raw_employee_data"]

    # Return the processed data resource
    return processed_employee_data(raw_data)


def load_bamboohr() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_bamboohr",
        destination="postgres",
        dataset_name="bamboohr_employee_data",
    )

    load_info = pipeline.run(bamboohr_source(), loader_file_format="csv")
    print(load_info)


if __name__ == "__main__":
    load_bamboohr()
