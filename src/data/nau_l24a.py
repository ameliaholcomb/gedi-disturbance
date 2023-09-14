from src import constants
from src.data import spark_postgis

new_cols = [
    "orbit",
    "sub_orbit",
    "beam",
    "lon_lm_a0",
    "lat_lm_a0",
    "agbd_a0",
    "rh_0_a0",
    "rh_5_a0",
    "rh_10_a0",
    "rh_15_a0",
    "rh_20_a0",
    "rh_25_a0",
    "rh_30_a0",
    "rh_35_a0",
    "rh_40_a0",
    "rh_45_a0",
    "rh_50_a0",
    "rh_55_a0",
    "rh_60_a0",
    "rh_65_a0",
    "rh_70_a0",
    "rh_75_a0",
    "rh_80_a0",
    "rh_85_a0",
    "rh_90_a0",
    "rh_92_a0",
    "rh_95_a0",
    "rh_98_a0",
    "rh_99_a0",
    "rh_100_a0",
    "cov_0",
    "cov_5",
    "cov_10",
    "cov_15",
    "cov_20",
    "cov_25",
    "cov_30",
    "cov_35",
    "cov_40",
    "cov_45",
    "cov_50",
    "cov_55",
    "cov_60",
    "cov_65",
    "cov_70",
    "cov_75",
    "cov_80",
    "cov_85",
    "cov_90",
    "cov_95",
    "cov_100",
    "pai_0",
    "pai_5",
    "pai_10",
    "pai_15",
    "pai_20",
    "pai_25",
    "pai_30",
    "pai_35",
    "pai_40",
    "pai_45",
    "pai_50",
    "pai_55",
    "pai_60",
    "pai_65",
    "pai_70",
    "pai_75",
    "pai_80",
    "pai_85",
    "pai_90",
    "pai_95",
    "pai_100",
    "outlier_umd",
]
old_cols = [
    "t1_shot_number",
    "t1_agbd",
    "t1_absolute_time",
    "t1_beam_type",
    "t1_sensitivity",
    "t1_agbd_se",
    "t2_shot_number",
    "t2_agbd",
    "t2_absolute_time",
    "t2_beam_type",
    "t2_sensitivity",
    "t2_agbd_se",
    "geo_partition",
    "t1_geometry",
    "t2_geometry",
]


def get_new_cols_select(table_name: str, prefix: str):
    return ",".join(
        [f"{table_name}.{col} AS {prefix}_{col}" for col in new_cols]
    )


query = f"""
SELECT
    {",".join(old_cols)},
    {get_new_cols_select("nau_1", "t1")},
    {get_new_cols_select("nau_2", "t2")}
FROM neighbors_table AS shots
INNER JOIN nau_table AS nau_1 ON shots.t1_shot_number = nau_1.shot_num
INNER JOIN nau_table AS nau_2 ON shots.t2_shot_number = nau_2.shot_num
"""


def run_main():
    spark = spark_postgis.get_spark()
    coincident_shots = spark.read.parquet(
        (constants.RESULTS_PATH / "gedi_neighbors").as_posix()
    )
    coincident_shots.createOrReplaceTempView("neighbors_table")
    nau_shots = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv((constants.DATA_PATH / "GEDI" / "NAU").as_posix())
    )
    nau_shots.createOrReplaceTempView("nau_table")
    print(query)
    spark.sql(query).filter("t1_outlier_umd == 0").filter(
        "t2_outlier_umd == 0"
    ).write.mode("overwrite").format("geoparquet").save(
        "/maps/forecol/results/gedi_neighbors_nau_l24a"
    )


if __name__ == "__main__":
    run_main()
