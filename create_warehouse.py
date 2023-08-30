"""
Creating a data warehouse from a database
"""
from datetime import datetime
from pyspark.sql.functions import current_date, lit
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "mysql-connector-j-8.1.0/mysql-connector-j-8.1.0.jar") \
    .getOrCreate()


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
        dimension_df = dimension_df.withColumnRenamed('id','warehouse_id'). \
            withColumnRenamed('movie_id','warehouse_movie_id'). \
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

    df_to_write.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .save()


def main():
    """
    main function to localize execution
    """

    dim_belongs_to_collection = create_warehouse_df('belongs_to_collection')
    dim_cast = create_warehouse_df('cast')
    dim_crew = create_warehouse_df('crew')
    dim_genres = create_warehouse_df('genres')
    dim_keywords = create_warehouse_df('keywords')
    dim_movie_information = create_warehouse_df('movie_information')
    dim_production_company = create_warehouse_df('production_company')
    dim_production_countries = create_warehouse_df('production_countries')
    fact_ratings = create_warehouse_df('ratings', False)
    dim_spoken_languages = create_warehouse_df('spoken_languages')
    dim_statistics = create_warehouse_df('statistics')

    insert_to_mysql(dim_belongs_to_collection, 'dim_belongs_to_collection')
    insert_to_mysql(dim_cast, 'dim_cast')
    insert_to_mysql(dim_crew, 'dim_crew')
    insert_to_mysql(dim_genres, 'dim_genres')
    insert_to_mysql(dim_keywords, 'dim_keywords')
    insert_to_mysql(dim_movie_information, 'dim_movie_information')
    insert_to_mysql(dim_production_company, 'dim_production_company')
    insert_to_mysql(dim_production_countries, 'dim_production_countries')
    insert_to_mysql(fact_ratings, 'fact_ratings')
    insert_to_mysql(dim_spoken_languages, 'dim_spoken_languages')
    insert_to_mysql(dim_statistics, 'dim_statistics')


if __name__ == '__main__':
    main()
