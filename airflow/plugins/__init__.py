from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.SchemaAndTableCreationOperator,
        operators.StageImmigrationDimensionsOperator,
        operators.StageImmigrationDataOperator,
        operators.StageTemperatureDataOperator,
        operators.CopyDataOperator,
        operators.CopyDimensionsOperator,
        operators.RunQualityCheckOperator,
        operators.RunAnalysisOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.ImmigrationDimensions
    ]
