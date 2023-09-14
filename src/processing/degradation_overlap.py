from typing import Callable
import pyspark.sql.types as T

shards_join_query = f"""
    SELECT s.*, f.shard_token AS shard_token
    FROM gedi_shots as s INNER JOIN degrade_shards as f
    ON ST_Contains(f.geometry, s.t2_geom)
"""


def to_pandas_points_query(points_column):
    return f"""
    SELECT *, ST_X({points_column}) AS longitude, ST_Y({points_column}) as latitude
    FROM gedi_shots
"""


def get_shots_df(spark, shots_dir):
    shots_df = spark.read.parquet(shots_dir.as_posix())
    shots_df.createOrReplaceTempView("gedi_shots")
    shots_df = spark.sql(
        "SELECT *, ST_GeomFromWKB(t1_geometry) AS t1_geom, ST_GeomFromWKB(t2_geometry) AS t2_geom FROM gedi_shots"
    )
    shots_df = shots_df.drop("t1_geometry", "t2_geometry")
    shots_df.createOrReplaceTempView("gedi_shots")
    return shots_df


def run_degradation_overlay(
    spark,
    shots_df,
    in_schema: T.StructType(),
    out_schema: T.StructType(),
    degradation_overlay_fn: Callable,
):
    return (
        spark
        # Replace geometry-style "t2_geom" column with pandas-friendly lon, lat
        # because Sedona does not yet support applyInGeopandas
        .sql(to_pandas_points_query("t2_geom"))
        # Choose only the minimal set of cols needed
        # because the next steps are quite processing-intensive
        .select(in_schema.fieldNames())
        # Use sharding appropriate for the degradation product
        # because we don't want I/O file thrashing
        .groupBy("shard_token").applyInPandas(
            degradation_overlay_fn,
            schema=out_schema,
        )
        # Rejoin with the complete set of columns
        .join(shots_df, on=["t1_shot_number", "t2_shot_number"], how="left")
    )
