from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

from ..utils.constants import END_DATE, START_DATE

start_date = START_DATE
end_date = END_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

daily_partitions = DailyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)