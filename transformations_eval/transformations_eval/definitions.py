from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import transformations_eval_dbt_assets
from .project import transformations_eval_project
from .schedules import schedules

defs = Definitions(
    assets=[transformations_eval_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=transformations_eval_project),
    },
)

