#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_pipeline_foundations.data_ingestion import DataIngestion
from data_pipeline_foundations.data_transformations import DataTransformations


app = cdk.App()
DataIngestion(app, "DataIngestion")
DataTransformations(app, "DataTransformations")

app.synth()
