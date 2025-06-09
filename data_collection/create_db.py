#%%
import duckdb
import pandas as pd
from pathlib import Path


#### Functions for constructing new tables. 
#WARNING: Drops previous instance of table with same name
def construct_navermap_review(conn:duckdb.DuckDBPyConnection, 
                              navermap_reviews_path:Path):
    # Import into pandas dataframe first (due to datatype conversion issues)
    navermap_reviews_df = pd.read_parquet(navermap_reviews_path)
    conn.execute("DROP TABLE IF EXISTS navermap_reviews;")
    conn.execute("CREATE OR REPLACE TABLE navermap_reviews AS SELECT * FROM navermap_reviews_df;")
    del navermap_reviews_df

def construct_restaurants(conn:duckdb.DuckDBPyConnection, 
                          restaurants_table_path:Path):
    restaurants_df = pd.read_parquet(restaurants_table_path)
    conn.execute("DROP TABLE IF EXISTS restaurants;")
    conn.execute("CREATE OR REPLACE TABLE restaurants AS SELECT * FROM restaurants_df;")
    del restaurants_df

#### Function for making a certain column into PRIMARY KEY
def make_column_primary_key(conn: duckdb.DuckDBPyConnection,
                            table_name: str,
                            column_name: str):
    constraint_name = f"{column_name}_pk"
    sql = f"""
    ALTER TABLE {table_name}
    ADD CONSTRAINT {constraint_name} PRIMARY KEY ({column_name});
    """
    conn.execute(sql)

def main(navermap_reviews_path = Path("../dataset/navermap_reviews_final.parquet.gzip"),
         restaurants_table_path = Path("G:/My Drive/Data/naver_search_results/restaurants_table.parquet")):
    # database connection
    with duckdb.connect("../dataset/reviews.db", read_only=False) as conn:
        # Construct table `restaurants`
        construct_restaurants(conn, restaurants_table_path)
        # Construct table `navermap_reviews`
        construct_navermap_review(conn, navermap_reviews_path)
        # Make `review_id` a primary key for navermap_reviews
        make_column_primary_key(conn, "navermap_reviews", "review_id")
        # Show current state of DB
        conn.sql("PRAGMA show_tables_expanded;").show()


if __name__ == "__main__":
     main()

# %%
