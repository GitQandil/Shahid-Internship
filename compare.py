"""
Creating a data warehouse from a database
"""
from datetime import datetime
from pyspark.sql.functions import current_date, lit, when
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "mysql-connector-j-8.1.0/mysql-connector-j-8.1.0.jar") \
    .getOrCreate()


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# hide info logs
quiet_logs(spark)


def create_df(dbtable, dbname, src=True):
    """
    creates a spark df from a database table
    :param dbtable: sql Table
    :return: spark dataframe
    """

    high_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
    url = f"jdbc:mysql://localhost:3306/{dbname}"
    user = 'root'
    password = '9VT_DngnWXB7Li'

    dimension_df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password).load()

    if src:
        dimension_df = dimension_df.withColumnRenamed('id', 'src_id'). \
            withColumnRenamed('movie_id', 'src_movie_id'). \
            withColumn("src_start_date", current_date()). \
            withColumn("src_end_date", lit(high_date))

    return dimension_df


def insert_to_mysql(df_to_write, table_name, dbname):
    """
    insert spark dataframe into mysql

    :param df_to_write: spark dataframe
    :param table_name: string
    """
    url = f"jdbc:mysql://localhost:3306/{dbname}"
    user = 'root'
    password = '9VT_DngnWXB7Li'

    df_to_write.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/data_warehouse") \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .save()


def compare_src(src_table, warehouse_table):
    """
    Compare Source table to Data Warehouse table
    :param src_table: spark df
    :param warehouse_table: spark df
    :return: spark df
    """
    df_merge = warehouse_table.join(src_table, (src_table.src_id == warehouse_table.warehouse_id) &
                                    (src_table.src_end_date == warehouse_table.end_date), how='fullouter')

    df_merge = df_merge.withColumn('action', when(df_merge.warehouse_id != df_merge.src_id, 'UPSERT') \
                                   .when(df_merge.src_id.isNull() & df_merge.is_active, 'DELETE') \
                                   .when(df_merge.warehouse_id.isNull(), 'INSERT').otherwise('NOACTION'))

    df_merge_p1 = df_merge.filter(
        df_merge.action == 'NOACTION').select(df_merge.columns)

    # df_merge_p2 = df_merge.filter(df_merge.action == "INSERT").select(
    #     df_merge.sid.alias("sid"),
    #     df_merge.src_department_tk.alias("department_tk"),
    #     df_merge.src_department_bk.alias("department_bk"),
    #     df_merge.src_department_name.alias("department_name"),
    #     lit(True).alias("is_current"),
    #     lit(False).alias("is_deleted"),
    #     df_merge.src_start_date.alias("start_date"),
    #     df_merge.src_end_date.alias("end_date"),
    # )


def main():
    """
    main function to localize execution
    """
    src_belongs_to_collection = create_df('belongs_to_collection', 'mbc_training')
    src_cast = create_df('cast', 'mbc_training')
    src_crew = create_df('crew', 'mbc_training')
    src_genres = create_df('genres', 'mbc_training')
    src_keywords = create_df('keywords', 'mbc_training')
    src_information = create_df('movie_information', 'mbc_training')
    src_company = create_df('production_company', 'mbc_training')
    src_countries = create_df('production_countries', 'mbc_training')
    src_ratings = create_df('ratings', 'mbc_training')
    src_spoken_languages = create_df('spoken_languages', 'mbc_training')
    src_statistics = create_df('statistics', 'mbc_training')

    dim_belongs_to_collection = create_df('belongs_to_collection', 'data_warehouse', False)
    dim_cast = create_df('cast', 'data_warehouse', False)
    dim_crew = create_df('crew', 'data_warehouse', False)
    dim_genres = create_df('genres', 'data_warehouse', False)
    dim_keywords = create_df('keywords', 'data_warehouse', False)
    dim_movie_information = create_df('movie_information', 'data_warehouse', False)
    dim_production_company = create_df('production_company', 'data_warehouse', False)
    dim_production_countries = create_df('production_countries', 'data_warehouse', False)
    fact_ratings = create_df('ratings', 'data_warehouse', False)
    dim_spoken_languages = create_df('spoken_languages', 'data_warehouse', False)
    dim_statistics = create_df('statistics', 'data_warehouse', False)


if __name__ == '__main__':
    main()
