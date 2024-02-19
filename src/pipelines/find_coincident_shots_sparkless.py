import argparse
import datetime
import geopandas as gpd
import os
import sqlalchemy as sa

from src import constants
from src.processing import coincidence_query


def execute(region, outdir, check_plan):

    query = coincidence_query.get_query_nopartition(region)
    # Connect to the PostGIS database
    conn = sa.create_engine(constants.DB_CONFIG).connect()

    if check_plan:
        explain_query = "EXPLAIN " + query
        res = conn.execute(sa.text(explain_query))
        for r in res:
            print(r[0])
        print("\n\n\n")
        yn = input("Proceed with query? (y/n):")
        if yn.lower() != "y":
            print("Exiting")
            return

    print(f"[{datetime.datetime.now()}] Starting Postgres query ...")
    chunks = gpd.read_postgis(
        sa.text(query), conn, geom_col="t2_geometry", chunksize=10000
    )
    print(f"[{datetime.datetime.now()}] Writing chunks to parquet ...")
    i = 0
    for chunk in chunks:
        chunk.to_parquet(outdir + f"/chunk_{i}.parquet")
        i = i + 1
    print(f"[{datetime.datetime.now()}] Done.")


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
        "--check_plan",
        help="Check the query plan before executing",
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(check_plan=False)

    args = parser.parse_args()

    region = gpd.read_file(args.region).geometry
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    execute(region, args.outdir, args.check_plan)


if __name__ == "__main__":
    run_main()
