## Imports
import ast
from importlib.resources import path
import os
import json
import logging
import argparse
import apache_beam as beam
from beam_nuggets.io import relational_db
from apache_beam import window 
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from click import parser
## config keys
from pipeline_config import (
    INPUT_SUBSCRIPTION,
    SERVICE_ACCOUNT_PATH,
    SOURCE_CONFIG_PROD,
    TABLE_CONFIG
)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(
#     SERVICE_ACCOUNT_PATH)

parser = argparse.ArgumentParser()
path_args, pipeline_args = parser.parse_known_args()



def run(path_arguments, pipeline_argumemts):
    """Pipeline function"""
    
    options = PipelineOptions(pipeline_argumemts)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    P = beam.Pipeline(options=options)

    try: 
        main_pipeline = (
            P
            | "Read data from pub sub"
            >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "Decode from bytes"
            >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | "Window data"
            >> beam.WindowInto(window.FixedWindows(300))
            | "Write into database"
            >> relational_db.Write(
                source_config=SOURCE_CONFIG_PROD,
                table_config=TABLE_CONFIG
            )
        )

        result = P.run()
        result.wait_until_finish()
    except Exception as e:
        print("The error is {}".format(e))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(path_args, pipeline_args)
