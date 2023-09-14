import argparse
import os
import pathlib

from src.data import spark_postgis
from src.data import radd_parser
from src.processing import degradation_overlap


def run_main(shots_dir=None, out_dir=None):
    spark = spark_postgis.get_spark()
    shots_df = degradation_overlap.get_shots_df(spark, shots_dir)
    shots_df = radd_parser.convert_shot_dates(shots_df)
    shots_df.createOrReplaceTempView("gedi_shots")

    degrade_shards_df = spark.createDataFrame(radd_parser.get_sharding_geoms())
    degrade_shards_df.createOrReplaceTempView("degrade_shards")

    shots_df = spark.sql(degradation_overlap.shards_join_query)
    shots_df.createOrReplaceTempView("gedi_shots")

    shots_df = degradation_overlap.run_degradation_overlay(
        spark=spark,
        shots_df=shots_df,
        in_schema=radd_parser.in_schema,
        out_schema=radd_parser.out_schema,
        degradation_overlay_fn=radd_parser.get_degradation_event_dates_for_shot_pair,
    )

    shots_df.filter(
        "(measured_disturbance + control_disturbance) > 0"
    ).write.mode("overwrite").format("parquet").save(out_dir.as_posix())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find GEDI shot pairs with interceding degradation, as detected by the RADD product."
    )
    parser.add_argument(
        "--shots_dir",
        "-s",
        help=("Directory in which to find table of coincident shots."),
        type=str,
        required=True,
    )
    parser.add_argument(
        "--out_dir",
        "-o",
        help=("Output directory."),
        type=str,
        required=True,
    )
    parser.add_argument(
        "--overwrite",
        help=("Whether or not to overwrite existing out_dir contents."),
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(overwrite=False)

    args = parser.parse_args()

    shots_dir = pathlib.Path(args.shots_dir)
    if not shots_dir.exists():
        print(f"No such directory: {shots_dir.as_posix()}")
        exit(1)

    out_dir = pathlib.Path(args.out_dir)
    if not out_dir.exists():
        os.mkdir(out_dir)
    if not out_dir.is_dir():
        print(f"Error: {out_dir} is not a directory.")
        exit(1)
    if any(out_dir.iterdir()) and not args.overwrite:
        print(f"Error: {out_dir} is not empty, but overwrite=False.")
        exit(1)

    run_main(
        shots_dir=shots_dir,
        out_dir=out_dir,
    )
