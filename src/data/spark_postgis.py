import geopandas as gpd
import pyproj
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
import math
from typing import Any, Dict
from src import constants

PARTITION_OPTS = set(["numPartitions", "partitionColumn", "lowerBound", "upperBound"])


def get_spark():
    """Set up a Sedona Spark instance that can interoperate with PostGIS."""
    spark = (
        SparkSession.builder.appName("degradationSample")
        .config("spark.serializer", KryoSerializer.getName)
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.3.1-incubating,"
            "org.datasyslab:geotools-wrapper:1.3.0-27.2,"
            "net.postgis:postgis-jdbc:2021.1.0,"
            "net.postgis:postgis-geometry:2021.1.0,"
            "org.postgresql:postgresql:42.5.4,",
        )
        # The PostGIS DriverWrapper requires a postgres driver be available
        .config(
            "spark.driver.extraClassPath",
            "/home/ah2174/.ivy2/jars/org.postgresql_postgresql-42.5.4.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "/home/ah2174/.ivy2/jars/net.postgis_postgis-geometry-2021.1.0.jar",
        )
        .config("spark.eventLog.enabled", True)
        .config("spark.eventLog.dir", "/maps/spark/logs")
        # Shapefile WKT strings can get very long -- show more debugging info
        .config("spark.sql.debug.maxToStringFields", 10000)
        # # Usually, if using this much memory on results and serialization,
        # # there's a design problem. But sometimes increasing these can help debug it.
        # .config("spark.driver.maxResultSize", "10g")
        # .config("spark.kryoserializer.buffer.max", "1g")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "32g")
        # No. of partitions given to the result of a shuffle by default.
        # (i.e. output of any rekeying of data)
        .config("spark.default.parallelism", 200)
        # Do not allow Spark to automatically coalesce partitions after a shuffle
        # when it thinks that the data size is small enough to fit in one partition.
        # Often, we are using a large number of partitions to distribute
        # large data resources that Spark does not know about.
        # Call coalese() explicitly after a shuffle when this behavior is desired.
        # https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", False)
        # Configure max number of concurrent tasks and allowable task failures
        # for spark in local mode.
        # https://spark.apache.org/docs/2.2.0/submitting-applications.html#master-urls
        # Format: local[num_tasks, num_failures]
        # N.b. Restrict number of concurrent tasks to
        #       1. Play nicely with other users on a shared machine
        #       2. Slow Spark down, esp. when tasks use resources spark doesn't know about.
        #           E.g. Querying a DB, making web requests, opening large files, etc.
        # N.b. Set max failures to 0 when developing. Increase for a production script.
        .master("local[15,0]")
        .getOrCreate()
    )

    # Consistently use the lon,lat (easting,northing) convention to specify coords
    # https://docs.geotools.org/latest/userguide/library/referencing/order.html
    # Bug: https://issues.apache.org/jira/browse/SEDONA-39
    # TL;DR ST_Transform currently uses lat,lon; while every other standard uses lon,lat
    spark.sparkContext.setSystemProperty("org.geotools.referencing.forceXY", "true")
    # spark.sparkContext.setLogLevel("INFO")
    SedonaRegistrator.registerAll(spark)
    return spark


def execute_read_query(spark: SparkSession, query: str, opts: Dict[str, Any] = None):
    spark_read = (
        spark.read.format("jdbc")
        # Using the protocol 'postgresql_postGIS' delegates to the PostGIS driver
        .option(
            "url",
            f"jdbc:postgresql_postGIS://{constants.DB_HOST}:{constants.DB_PORT}/{constants.DB_NAME}",
        )
        .option("query", query)
        # .option("dbtable", query)
        # PostGIS wrapper of the Postgres driver which adds postgis types
        .option("driver", "net.postgis.jdbc.DriverWrapper")
        .option("user", constants.DB_USER)
        .option("password", constants.DB_PASSWORD)
    )
    if opts:
        if set(opts.keys()) != PARTITION_OPTS:
            raise ValueError(
                "Invalid options: must include all of numPartitions, partitionColumn, lowerBound, and upperBound."
            )
        spark_read = (
            spark_read.option("numPartitions", opts["numPartitions"])
            .option("partitionColumn", opts["partitionColumn"])
            .option("lowerBound", opts["lowerBound"])
            .option("upperBound", opts["upperBound"])
        )

    return spark_read.load()


@udf(returnType=StringType())
def get_utm_projection(point):
    """Based on an ST_Point, return best UTM epsg-code.

    This function should be applied row-wise to create a
    target UTM projection for each geometry,
    allowing locally valid metric computations
    by replacing `geom' in an SQL query with:
        ST_Transform(geom, "epsg:4326", projection)
    as long as all geometries in the query share the same projection.
    """
    utm_band = str((math.floor((point.x + 180) / 6) % 60) + 1)
    if len(utm_band) == 1:
        utm_band = "0" + utm_band
    if point.y >= 0:
        epsg_code = "326" + utm_band
        return "epsg:" + epsg_code
    epsg_code = "327" + utm_band
    return "epsg:" + epsg_code


def sqlify_geometry(region: gpd.GeoSeries) -> str:
    """Create sql-friendly string version of geometry.

    Uses EPSG:4326 by default unless region.crs is set.
    """
    if not type(region) == gpd.GeoSeries:
        raise ValueError(
            "Bad region type. Maybe select the geometry column of your dataframe?"
        )

    if region.crs:
        crs = region.crs
        print(crs.to_epsg())
    else:
        crs = pyproj.CRS.from_user_input(constants.WGS84)
    # if crs.to_epsg() == None:
    #     raise ValueError("Unable to interpret CRS for region")
    # return f"ST_GeomFromText('{region.to_wkt().values[0]}', {crs.to_epsg()})"
    return f"ST_GeomFromText('{region.to_wkt().values[0]}', 4326)"
