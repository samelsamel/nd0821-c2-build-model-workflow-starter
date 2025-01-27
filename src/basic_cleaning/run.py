#!/usr/bin/env python
"""
Author: Amel Sellami 
Date creation: 27-04-2022

download raw data from w&b and apply some basic data cleaning and export results to a new artifact
"""
import argparse
import os
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """Download data and perform cleaning and save to w&b"""
    run = wandb.init(job_type="basic_cleaning", project="nyc_airbnb", group="eda", save_code=True)
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load data
    logger.info(f'Download artifact {args.input_artifact}')
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Read data

    logger.info('Reading data')
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price

    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # remove points outside the bounderies
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Save the data
    logger.info(f'saving dataframe {args.output_artifact}')
    df.to_csv("clean_sample.csv", index=False)

    # Init w&b artifact object
    logger.info('Upload to w&b the artifact')
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


    # Finish the run 
    os.remove(args.output_artifact)
    run.finish()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='name of the artifact input',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='name of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='the type of the output',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='description of the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='the minimum price',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='the maximum price',
        required=True
    )


    args = parser.parse_args()

    go(args)
