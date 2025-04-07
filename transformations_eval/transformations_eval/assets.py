from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import transformations_eval_project


@dbt_assets(manifest=transformations_eval_project.manifest_path)
def transformations_eval_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    

