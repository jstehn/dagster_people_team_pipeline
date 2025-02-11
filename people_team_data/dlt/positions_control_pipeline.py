from typing import Sequence

import dlt

from google_sheets import google_spreadsheet


def load_position_control(
    spreadsheet_url_or_id: str, range_names: Sequence[str]
) -> None:
    """
    Loads explicitly passed ranges
    """
    pipeline = dlt.pipeline(
        pipeline_name="position_control_pipeline",
        destination="postgres",
        dataset_name="position_control_data",
    )
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        range_names=range_names,
        get_sheets=False,
        get_named_ranges=False,
    )
    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    spreadsheet_url_or_id = dlt.config.get(
        "sources.position_control_pipeline.spreadsheet_url_or_id"
    )
    range_names = dlt.config.get(
        "sources.position_control_pipeline.range_names"
    )

    load_position_control(spreadsheet_url_or_id, range_names)
