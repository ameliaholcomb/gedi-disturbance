import geopandas as gpd
from src.data import spark_postgis


COLUMNS = [
    "shot_number",
    "geometry",
    "agbd",
    "absolute_time",
    "beam_type",
    "sensitivity",
    "agbd_se",
]


def get_columns(identifier: str) -> str:
    return ", ".join(
        [
            f"{identifier}.{column} as  {identifier}_{column}"
            for column in COLUMNS
        ]
    )


def get_query(region):
    # TODO(amelia): Remove granule_name query constraint
    # once the GEDI db has uniform versioning.
    return f"""
    (
    WITH FilteredTable AS NOT MATERIALIZED (
        SELECT 
          *,
          -- Try to split up work evenly while maintaining spatial locality.
          -- Group shots by their 2x2 degree square, 
          -- and map pseudo-randomly to a partition id.
          MOD(MOD(hashint4(
            (TRUNC(ST_X(geometry) / 2) * 360 + TRUNC(ST_Y(geometry) / 2))::int
            ), 1000) + 1000, 1000) AS geo_partition
        FROM level_4a 
        WHERE 1=1
            AND ST_Intersects(geometry, {spark_postgis.sqlify_geometry(region)})
            AND degrade_flag = 0
            AND l2_quality_flag = 1
            AND l4_quality_flag = 1
            AND granule_name ~ '.*01_V002\.h5'
    )
    SELECT {get_columns('t1')}, {get_columns('t2')}, t1.geo_partition
    FROM FilteredTable AS t1 JOIN FilteredTable AS t2
        ON 1=1
        AND ST_DWITHIN(t1.geometry::geography, t2.geometry::geography, 40, FALSE)
        AND t1.shot_number < t2.shot_number
    ) AS coincident_shots
    """


opts = {
    "numPartitions": 56,
    "partitionColumn": "geo_partition",
    "lowerBound": 0,
    "upperBound": 1000,
}

