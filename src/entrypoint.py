import xml.etree.ElementTree as ET
import sqlite3
from datetime import datetime

from lxml import etree
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType
import os


class InMemoryState(object):

    def __init__(self):
        self.connection = sqlite3.connect(os.path.join(os.curdir, os.pardir, "database/aspen.db"))
        cursor = self.connection.cursor()
        count = None
        try:
            count = cursor.execute("select count(*) from projections").fetchone()[0]
        except Exception:
            print("No db found creating new one")
        if not count:
            sql_file = open(os.path.join(os.curdir, os.pardir, "database/init.sql"))
            sql_as_string = sql_file.read()
            cursor.executescript(sql_as_string)


def explode_projections(df, spark):
    """
    function to explode the projections xml data
    :param df:
    :return:
    """
    # udf to check if a column has text
    parse_xml_udf = udf(parse_xml, ArrayType(ArrayType(StringType())))
    # spark.udf.register("parse_xml", parse_xml_udf)
    df = df.select(df['ProjectionID'], df['StartDate'], explode(parse_xml_udf(df['XML'])).alias("exploded"))
    df = df.withColumn("Deposit", df["exploded"].getItem(0)) \
        .withColumn("Description", df["exploded"].getItem(1)) \
        .withColumn("MonthYear", df["exploded"].getItem(2)) \
        .withColumn("Payment", df["exploded"].getItem(3)) \
        .withColumn("Projected", df["exploded"].getItem(4)) \
        .drop(df["exploded"])
    return df


def parse_xml(XML):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(XML, parser=parser)
    results = []
    for b in root.find('{urn:schemas-microsoft-com:rowset}data').getchildren():
        sub_array = []
        sub_array.append(b.get('Deposit', None))
        sub_array.append(b.get('Description', None))
        sub_array.append(b.get('MonthYear', None))
        sub_array.append(b.get('Payment', None))
        sub_array.append(b.get('Projected', None))
        results.append(sub_array)
    return results

def chart(spark,actuals_df,projection_df ):

    actuals_df.createOrReplaceTempView("actuals")
    projection_df.createOrReplaceTempView("projections")

    return spark.sql("""select a.ProjectionID as Projection, p.StartDatefrom as `Month/Year`, a.Memo as Activity, p.Deposit as Estimated, a.Amount as Actual, p.Projected as `Estimated Balance`
      from actuals a join projections p on a.ProjectionID = p.ProjectionID;""")


# # parse xml tree, extract the records and transform to new RDD
# records_rdd = file_rdd.flatMap(parse_xml)
#
# # convert RDDs to DataFrame with the pre-defined schema
# book_df = records_rdd.toDF(my_schema)

if __name__ == '__main__':
    # load the db with init script.
    db = InMemoryState().connection

    # now try to load the databases and tables into spark
    import os

    jdbc_driver_path = os.path.abspath(os.path.join(os.curdir, os.pardir, "jars/sqlite-jdbc-3.32.3.2.jar"))
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--conf spark.executor.extraClassPath={} --driver-class-path {} --jars file://{} pyspark-shell'.format(
        jdbc_driver_path, jdbc_driver_path, jdbc_driver_path)
    spark = SparkSession.builder.getOrCreate()
    # spark.sparkContext.setLogLevel('DEBUG')

    actuals_df = spark.read.format('jdbc'). \
        options(url='jdbc:sqlite:{}'.format(os.path.abspath(os.path.join(os.curdir, os.pardir, "database/aspen.db"))),
                dbtable='Actuals', driver='org.sqlite.JDBC').load()

    projection_df = spark.read.format('jdbc'). \
        options(url='jdbc:sqlite:{}'.format(os.path.abspath(os.path.join(os.curdir, os.pardir, "database/aspen.db"))),
                dbtable='Projections', driver='org.sqlite.JDBC').load()

    actuals_df.show()
    projection_df = explode_projections(projection_df, spark)
    projection_df.printSchema()
    projection_df.show(20, False)
    chart_df = chart(spark,actuals_df,projection_df)
    chart_df.show(20, False)
