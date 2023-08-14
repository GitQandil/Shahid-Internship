"""
responsible for cleaning multiple csv files, exploding of json
columns and turning them into a dataframe,inserting result dataframes
into a predefined database schema
"""

from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
from utilities import utils


def main():
    """
    :return: main
    """
    meta_df = pd.read_csv("data/movies_metadata.csv", low_memory=False)

    meta_df.drop_duplicates(inplace=True)

    drop_ruined = meta_df[~meta_df.adult.isin(['True', 'False'])].index.to_list()

    meta_df.drop(drop_ruined, inplace=True)

    meta_df.rename(columns={'id': 'movie_id'}, inplace=True)

    meta_df.imdb_id = meta_df.imdb_id.astype(str).apply(utils.replace_tt) \
        .apply(pd.to_numeric, errors='coerce')

    meta_df = meta_df.astype({"popularity": "float64", "budget": "float",
                              "movie_id": "Int64", "imdb_id": "Int64"})

    meta_df.release_date = pd.to_datetime(meta_df['release_date'], errors='coerce')

    meta_df = utils.na_ld(meta_df,
                          [col for col in meta_df.columns if meta_df[col].dtype == 'O'])

    stats_df = meta_df[["movie_id", "budget", "popularity", "revenue",
                        "vote_average", "vote_count"]]

    movie_info = meta_df[["movie_id", "title", "original_title", "adult",
                          "homepage", "original_language", "overview",
                          "poster_path", "tagline", "status", "release_date", "imdb_id"]]

    production_companies = utils.normalize_all(meta_df, 'production_companies', 'movie_id')
    production_countries = utils.normalize_all(meta_df, 'production_countries', 'movie_id')
    belongs_to_collection = utils.normalize_all(meta_df, 'belongs_to_collection', 'movie_id', False)
    spoken_languages = utils.normalize_all(meta_df, 'spoken_languages', 'movie_id')
    genres_df = utils.normalize_all(meta_df, 'genres', 'movie_id')

    movie_info.drop_duplicates(inplace=True)
    stats_df.drop_duplicates(inplace=True)

    production_companies.rename(columns={'id': 'company_id'}, inplace=True)
    production_companies = production_companies.astype({"company_id": "Int64"})

    belongs_to_collection.rename(columns={'id': 'collection_id'}, inplace=True)
    genres_df.rename(columns={'id': 'genre_id'}, inplace=True)

    # credits
    credits_df = pd.read_csv('data/credits.csv', dtype={'id': 'Int64'})

    credits_df.drop_duplicates(inplace=True)

    credits_df = utils.na_ld(credits_df, ['cast', 'crew'])

    credits_df.rename(columns={'id': 'movie_id'}, inplace=True)

    cast_df = utils.normalize_all(credits_df, 'cast', 'movie_id')
    crew_df = utils.normalize_all(credits_df, 'crew', 'movie_id')

    cast_df.rename(columns={'id': 'actor_id'}, inplace=True)
    crew_df.rename(columns={'id': 'crew_id'}, inplace=True)

    cast_df['gender'] = utils.categorize_gender(cast_df.gender)
    crew_df['gender'] = utils.categorize_gender(crew_df.gender)

    cast_df = cast_df.astype({"cast_id": "Int64", "actor_id": "Int64", "order": 'Int64'})
    crew_df = crew_df.astype({"crew_id": "Int64"})

    # keywords
    keywords_df = pd.read_csv('data/keywords.csv')

    keywords_df.drop_duplicates(inplace=True)

    keywords_df = utils.na_ld(keywords_df, ['keywords'])

    keywords_df.rename(columns={'id': 'movie_id'}, inplace=True)

    keywords_df = utils.normalize_all(keywords_df, 'keywords', 'movie_id')

    keywords_df.rename(columns={'id': 'keyword_id'}, inplace=True)

    # ratings
    ratings_df = pd.read_csv('data/ratings.csv')

    ratings_df.rename(columns={'userId': 'user_id', 'movieId': 'movie_id'}, inplace=True)

    ratings_df.drop_duplicates(inplace=True)

    ratings_df['date'] = pd.to_datetime(ratings_df['timestamp'] + 14400, unit='s')

    ratings_df.drop('timestamp', inplace=True, axis=1)

    ratings_not_links = set(ratings_df.movie_id.to_list()).difference(movie_info.movie_id.to_list())
    to_drop = ratings_df[ratings_df.movie_id.isin(ratings_not_links)].index.to_list()
    ratings_df.drop(to_drop, inplace=True)

    # inserting dataframes into database
    engine = create_engine("mysql+pymysql://root:"
                           f"{quote_plus('Jor@21795')}@localhost:3306/mbc_training")

    df_list = [movie_info, cast_df, crew_df,
               keywords_df, belongs_to_collection, genres_df,
               production_companies, production_countries,
               spoken_languages, ratings_df, stats_df]

    target_tables = ['movie_information', 'cast', 'crew',
                     'keywords', 'belongs_to_collection', 'genres',
                     'production_company', 'production_countries',
                     'spoken_languages', 'ratings', 'statistics']

    flag = False
    for _, (target_df, target_table) in enumerate(zip(df_list, target_tables)):
        if not flag:
            target_df.to_sql(target_table, con=engine,
                             if_exists='append', chunksize=10000, index=False)
            flag = True
        else:
            target_df.to_sql(target_table, con=engine,
                             if_exists='append', chunksize=10000, index_label='id')


if __name__ == '__main__':
    main()
