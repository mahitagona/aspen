import sqlite3

from lxml import etree
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType
import os


class InMemoryState(object):
    """
    function to connect init to sqlite db
    """
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


def chart(spark, actuals_df, projection_df):
    """
    function to utilise spark sql to obtain requested data
    :param spark:
    :param actuals_df:
    :param projection_df:
    :return:
    """
    #materializing the contents of the dataframes
    actuals_df.createOrReplaceTempView("actuals")
    projection_df.createOrReplaceTempView("projections")


    return spark.sql("""WITH a as (select a.ProjectionID as Projection, CONCAT(date_format(a.DateDeposited,'MMM') ," ",YEAR(a.DateDeposited)) as `Month/Year`, a.DateDeposited as DateDeposited, p.Deposit as `Deposit:Insurance Estimation`, p.Payment as Estimated, a.Amount as Actual, p.Projected as `Estimated Balance`
                        from actuals a join projections p on a.ProjectionID = p.ProjectionID 
                        and CONCAT(date_format(a.DateDeposited,'MMM') ," ",YEAR(a.DateDeposited)) = CONCAT(date_format(p.MonthYear,'MMM') ," ",YEAR(p.MonthYear))
                        where p.Description == 'Insurance' and 
                        a.Memo == 'Insurance'
                        order by a.DateDeposited ASC)
                        SELECT Projection, `Month/Year`, Activity ,Estimated, Actual, `Estimated Balance` FROM (
                        SELECT Projection, `Month/Year`, 'Withdrawl:Insurance' as Activity ,Estimated, Actual, `Estimated Balance`, DateDeposited 
                        FROM a 
                        UNION ALL
                        SELECT DISTINCT Projection, `Month/Year`, 'Deposit:Insurance' as Activity , `Deposit:Insurance Estimation`, NULL, NULL, DateDeposited 
                        FROM a)
                        ORDER BY DateDeposited, Activity ASC;""")

if __name__ == '__main__':
    # loaded the db with init script
    db = InMemoryState().connection

    #loaded the databases and tables into spark
    import os

    jdbc_driver_path = os.path.abspath(os.path.join(os.curdir, os.pardir, "jars/sqlite-jdbc-3.32.3.2.jar"))
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--conf spark.executor.extraClassPath={} --driver-class-path {} --jars file://{} pyspark-shell'.format(
        jdbc_driver_path, jdbc_driver_path, jdbc_driver_path)
    spark = SparkSession.builder.getOrCreate()
    # spark.sparkContext.setLogLevel('DEBUG')

    #created an actuals dataframe using sqlite jdbc connection
    actuals_df = spark.read.format('jdbc'). \
        options(url='jdbc:sqlite:{}'.format(os.path.abspath(os.path.join(os.curdir, os.pardir, "database/aspen.db"))),
                dbtable='Actuals', driver='org.sqlite.JDBC').load()

    #created an projection dataframe using sqlite jdbc connection
    projection_df = spark.read.format('jdbc'). \
        options(url='jdbc:sqlite:{}'.format(os.path.abspath(os.path.join(os.curdir, os.pardir, "database/aspen.db"))),
                dbtable='Projections', driver='org.sqlite.JDBC').load()

    actuals_df.show()
    projection_df = explode_projections(projection_df, spark)
    projection_df.printSchema()
    projection_df.show(20, False)
    chart_df = chart(spark, actuals_df, projection_df)
    chart_df.show(20, False)
