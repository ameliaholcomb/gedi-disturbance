import argparse
import geopandas as gpd
import psycopg2

from src import constants
from src.data import spark_postgis
from src.processing import coincidence_query


def execute(region, outdir, num_partitions, check_plan):
    if check_plan:
        # Connect to the PostGIS database
        conn = psycopg2.connect(
            database=constants.DB_NAME,
            user=constants.DB_USER,
            password=constants.DB_PASSWORD,
            host=constants.DB_HOST,
            port=constants.DB_PORT,
        )
        query = coincidence_query.get_query(region)
        explain_query = f"EXPLAIN SELECT * FROM {query} WHERE geo_partition > 180 AND geo_partition <= 200;"
        cur = conn.cursor()
        # cur.execute("SET max_parallel_workers_per_gather = 0;")
        cur.execute(explain_query)
        res = cur.fetchall()
        for r in res:
            print(r[0])

        print("\n\n\n")
        yn = input("Proceed with query? (y/n): ")
        if yn.lower() != "y":
            print("Exiting")
            return

    spark = spark_postgis.get_spark()
    coincidence_query.opts["numPartitions"] = num_partitions

    df = spark_postgis.execute_read_query(
        spark,
        coincidence_query.get_query(region),
        opts=coincidence_query.opts,
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
    parser.add_argument(
        "--check_plan",
        help="Check the query plan before executing",
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(check_plan=False)

    args = parser.parse_args()

    region = gpd.read_file(args.region).geometry
    execute(region, args.outdir, args.num_partitions, args.check_plan)


if __name__ == "__main__":
    run_main()
