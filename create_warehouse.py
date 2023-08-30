"""
Creating a data warehouse from a database
"""
from datetime import datetime
from pyspark.sql.functions import current_date, lit
from pyspark.sql.session import SparkSession, SparkConf
import pyspark
from concurrent.futures import ThreadPoolExecutor



config = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '8'),
                                     ('spark.cores.max', '8'), ('spark.driver.memory','8g'),
                                     ("spark.jars", "mysql-connector-j-8.1.0/mysql-connector-j-8.1.0.jar")])


sc = pyspark.SparkContext(conf=config)
spark = SparkSession.builder.config(conf=config).getOrCreate()

def create_warehouse_df(dbtable, is_dim=True):
    """
    creates a spark df from a database table
    :param dbtable: sql Table
    :param is_dim: bool
    :return: spark dataframe
    """
    high_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
    url = "jdbc:mysql://localhost:3306/mbc_training"
    user = 'root'
    password = '9VT_DngnWXB7Li'

    dimension_df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password).load()

    if is_dim:
        dimension_df = dimension_df.withColumnRenamed('id', 'warehouse_id'). \
            withColumnRenamed('movie_id', 'warehouse_movie_id'). \
            withColumn('date_created', current_date()). \
            withColumn("start_date", current_date()). \
            withColumn("end_date", lit(high_date)). \
            withColumn('is_active', lit(True))

    return dimension_df


def insert_to_mysql(df_to_write, table_name):
    """
    insert spark dataframe into mysql
    :param df_to_write: spark dataframe
    :param table_name: string
    """

    url = "jdbc:mysql://localhost:3306/data_warehouse"
    user = 'root'
    password = '9VT_DngnWXB7Li'

    df_to_write.repartition(15)\
        .write \
        .mode('overwrite') \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .save()



dim_belongs_to_collection = create_warehouse_df('belongs_to_collection')
dim_cast = create_warehouse_df('cast')
dim_crew = create_warehouse_df('crew')
dim_genres = create_warehouse_df('genres')
dim_keywords = create_warehouse_df('keywords')
dim_movie_information = create_warehouse_df('movie_information')
dim_movie_information.withColumnRenamed('warehouse_movie_id', 'warehouse_id')
dim_production_company = create_warehouse_df('production_company')
dim_production_countries = create_warehouse_df('production_countries')
fact_ratings = create_warehouse_df('ratings', False)
dim_spoken_languages = create_warehouse_df('spoken_languages')
dim_statistics = create_warehouse_df('statistics')

dataframes = [dim_belongs_to_collection, dim_cast, dim_crew,dim_genres,
              dim_keywords,dim_movie_information, dim_production_company,
              dim_production_countries, fact_ratings, dim_spoken_languages,
              dim_statistics]

table_names = ["dim_belongs_to_collection", "dim_cast", "dim_crew",
               "dim_genres","dim_keywords","dim_movie_information",
               "dim_production_company","dim_production_countries","fact_ratings",
               "dim_spoken_languages","dim_statistics"]

with ThreadPoolExecutor() as executor:
    executor.map(insert_to_mysql, dataframes, table_names)





