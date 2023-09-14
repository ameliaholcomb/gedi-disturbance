import argparse
import geopandas as gpd

from src.data import spark_postgis
from src.processing import coincidence_query


def execute(region, outdir, num_partitions):
    spark = spark_postgis.get_spark()
    coincidence_query.opts["numPartitions"] = num_partitions
    df = spark_postgis.execute_read_query(
        spark, coincidence_query.get_query(region), opts=coincidence_query.opts
    )
    df.write.mode("overwrite").format("geoparquet").save(outdir)


def run_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--region",
        type=str,
        help="Path to shapefile defining region of interest",
        default="",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        help="Path to output directory",
        default="",
    )
    parser.add_argument(
        "--num_partitions",
        type=int,
        help="Number of partitions to use",
        default=56,
    )

    args = parser.parse_args()

    region = gpd.read_file(args.region).geometry
    execute(region, args.outdir, args.num_partitions)


if __name__ == "__main__":
    run_main()
