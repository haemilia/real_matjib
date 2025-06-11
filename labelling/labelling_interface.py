#%%
import argparse
from pathlib import Path
import duckdb
import pandas as pd
import os
import typing

# Parsing arguments
def parse_arguments()-> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="Labeller",
                                     description= "Program that helps user to label review data.",
                                     )
    parser.add_argument("-r", "--review_type",
                        choices=["map", "navermap_reviews", "blog", "naverblog_reviews"],
                        default="map",
                        help="Option to decide which review dataset to use; default choice is map")
    args = parser.parse_args()
    return args

# Read list of restaurants from DB
def read_restaurants_from_db(conn:duckdb.DuckDBPyConnection, restaurants_table_name:str="restaurants") -> pd.DataFrame:
    """
    Retrieves 'restaurants' table from DuckDB database
    Args:
        conn: Conncection to database file
        restaurants_table_name(str): Name of the table to read; Default: "restaurants"
    Returns:
        A pandas DataFrame representation of the table contents.
    """
    df = conn.table(restaurants_table_name).df()
    return df

# Sample from list of restaurants
def sample_restaurants(df:pd.DataFrame, 
                       random_seed:int|None=None,
                       sample_size:int=100) -> pd.DataFrame:
    sampled_df = df.sample(n=sample_size, random_state=random_seed, axis=0)
    return sampled_df

# Function that constructs the restaurant page URL from id
def restaurant_page_url(id:str) -> str:
    url = f'https://m.place.naver.com/restaurant/{id}/review/visitor'
    return url

# Read list of review ids that are from the sampled restaurants, 
# and store the information as a file
def read_sampled_reviews(conn:duckdb.DuckDBPyConnection,
                         sampled_restaurants:pd.DataFrame,
                         table_name:str,
                         restaurants_table_name:str):
    sampled_restaurant_ids = tuple(sampled_restaurants["naver_store_id"].values)
    q0 = f"""SELECT *
            FROM {table_name} AS r
            JOIN {restaurants_table_name} AS rest
            ON r.store_id = rest.naver_store_id
            WHERE rest.naver_store_id IN {sampled_restaurant_ids};
        """
    sampled_reviews = conn.execute(q0).fetch_df()
    return sampled_reviews

# Create and prepare table for labelled data
def prepare_labelled_reviews_table(conn:duckdb.DuckDBPyConnection,
                                   sampled_reviews_df:pd.DataFrame,
                                   labelled_table_name:str,
                                   labelled_column_name:str):
    # Clean any past data
    q0 = f"DROP TABLE {labelled_table_name} IF EXISTS;"
    conn.execute(q0)
    # Make relation object from df
    sampled_reviews_rel = conn.from_df(sampled_reviews_df)
    # Create table from relation object
    sampled_reviews_rel.create(labelled_table_name)
    # Add new label column
    LABELLED_COLUMN_TYPE = "BOOLEAN"
    q1 = f"ALTER TABLE {labelled_table_name} ADD COLUMN {labelled_column_name} {LABELLED_COLUMN_TYPE};"
    conn.execute(q1)
# Check if we're repeating
def we_need_sampling(conn:duckdb.DuckDBPyConnection, 
                     sampled_table_name:str):
    # 1. Check if the table exists
    # DuckDB's PRAGMA table_info is a concise way to check for table existence.
    # If the table doesn't exist, this query will return an empty result.
    # Alternatively, you can query information_schema.tables.
    try:
        q1 = f"PRAGMA table_info('{sampled_table_name}');"
        table_info = conn.execute(q1).fetchdf()
        if table_info.empty:
            print(f"Table '{sampled_table_name}' does not exist.")
            return True
    except duckdb.ConnectionException as e:
        # PRAGMA table_info generally doesn't throw an error for non-existent table,
        # but other errors during execution might occur.
        print(f"Error checking for table '{sampled_table_name}' existence: {e}")
        return True

    # 2. Check if the table has any rows
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {sampled_table_name};").fetchone()
        if result is not None and len(result) > 0:
            row_count = result[0]
            if row_count > 0:
                return False
            
        print(f"Table '{sampled_table_name}' exists but is empty (0 rows).")
        return True
    except duckdb.ConnectionException as e:
        # This catch might be redundant if table_info already verified existence,
        # but good for robustness against other SQL execution errors.
        print(f"Error checking row count for table '{sampled_table_name}': {e}")
        return True
    except Exception as e:
        print(f"An unexpected error occurred while checking table '{sampled_table_name}' data: {e}")
        return True
    
# Update/insert labelled data
def update_labelled_reviews(conn:duckdb.DuckDBPyConnection,
                            labelled_table_name:str,
                            review_id_column_name:str,
                            review_id_value:str,
                            labelled_column_name:str,
                            new_value:bool):
    update_sql = f"""
    UPDATE {labelled_table_name}
    SET "{labelled_column_name}" = ?
    WHERE "{review_id_column_name}" = ?;
    """
    try:
        cursor = conn.execute(update_sql, (new_value, review_id_value))
        # Check how many rows were affected
        rows_affected = cursor.rowcount
        if rows_affected > 0:
            return True
        else:
            print(f"No rows found or updated for {review_id_column_name}={review_id_value}. Check if PK exists.")
            return False
    except duckdb.ConnectionException as e:
        print(f"DuckDB error during update: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during update: {e}")
        return False

###### Functions for terminal UI
def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def process_label_information(conn:duckdb.DuckDBPyConnection, 
                              labelled_table_name:str,
                              labelled_column_name:str,
                              review_id_column_name:str,
                              review_id_value:str,
                              user_input:str) -> bool:
    try:
        label_input = bool(int(user_input))
        update_labelled_reviews(conn, 
                                labelled_table_name=labelled_table_name,
                                labelled_column_name=labelled_column_name,
                                review_id_column_name=review_id_column_name,
                                review_id_value=review_id_value,
                                new_value=label_input)
    except Exception as e:
        print(f"An unexpected error occurred during update: {e}")
        return False
    else:
        return True

def loop_prompt_until(prompt:str, condition:typing.Callable[[str|None], bool]):
    user_input = input(prompt)
    while not condition(user_input):
        user_input = input("Wrong input! " + prompt)
    return user_input


# Simple input UI loop:
#   - When going into a new restaurant, say that we're doing so, and display the URL
#   - Show information about the review (text, date, author username)
def main(db_path=Path(__file__).parent / ".." / "dataset" / "reviews.db",
         restaurants_table_name= "restaurants",
         review_type:str= "map",
         labelled_column_name:str="is_advert"):
    
    if review_type in ["map", "navermap_reviews"]:
        table_name = "navermap_reviews"
        id_name = "review_id"
    elif review_type in ["blog", "naverblog_reviews"]:
        table_name = "naverblog_reviews"
        id_name = "post_id"
    else:
        assert review_type in ["map", "blog", "navermap_reviews", "naverblog_reviews"]
    # Announce beginning of program
    print("--- LABELLER ---")
    print("LOADING..." + "\n"*3)
    # DB connection initialisation
    with duckdb.connect(str(db_path)) as conn:
        labelled_table_name = f"{table_name}_labelled"
        needs_new_sample = we_need_sampling(conn, labelled_table_name)
        if needs_new_sample:
            print("Reading tables from DB...")
            # Read restaurants
            restaurants = read_restaurants_from_db(conn)
            print("Sampling...")
            # Sample restaurants
            sampled_restaurants = sample_restaurants(restaurants, random_seed=86)
            # Get sampled reviews
            sampled_reviews = read_sampled_reviews(conn, sampled_restaurants, table_name, restaurants_table_name)
            # Back up samples to DB, and prepare for labelling
            print("Backing up samples...")
            
            prepare_labelled_reviews_table(conn,
                                        sampled_reviews_df=sampled_reviews,
                                        labelled_table_name=labelled_table_name,
                                        labelled_column_name=labelled_column_name)
            
            sampled_reviews = conn.table(labelled_table_name).to_df() #Sync sampled_reviews with DB            
        else:
            sampled_reviews = conn.table(labelled_table_name).to_df()

        print("Preparation finished")
        clear_console() # Clear console before labelling


        all_reviews_num = len(sampled_reviews)
        # Determine the initial starting index for the user
        initial_start_index = 0
        # If we loaded existing data OR just created a new one, find the first unlabelled review
        # For a new sample, all values in 'is_advert' will be NULL/NaN
        unlabelled_reviews = sampled_reviews[sampled_reviews[labelled_column_name].isnull()]
        if not unlabelled_reviews.empty:
            # Get the integer position of the first unlabelled review
            initial_start_index = sampled_reviews.index.get_loc(unlabelled_reviews.index[0])
            if not needs_new_sample: # Only print resume message if it's actually resuming from past work
                print(f"Resuming labelling from review at index {initial_start_index} (first unlabelled).")
        else:
            if not needs_new_sample: # If not a new sample, and no unlabelled reviews
                print("All reviews in the existing sample appear to be labelled. Starting from 0.")
            else: # New sample, and somehow no unlabelled (shouldn't happen if column just added)
                print("New sample prepared, starting from 0.")
            initial_start_index = 0
        print(f"Total number of reviews:{all_reviews_num}")


        def begin_input_valid(begin_input)-> bool:
            try:
                if begin_input.isdigit() and 0<= int(begin_input) < all_reviews_num:
                    return True
                elif begin_input == "":
                    return True
            except Exception:
                pass
            return False
        # Use the dynamically determined initial_start_index as the default
        begin_prompt = f"Where would you like to start? (Default: {initial_start_index}) (Indexing starts at 0): "
        user_input_start = loop_prompt_until(begin_prompt, begin_input_valid)
        
        # Set the actual starting index for the loop
        int_begin_input = int(user_input_start) if user_input_start else initial_start_index

        # Labelling Loop
        while True:
            clear_console()
            print("-"*20 + " LABELLING LOOP "+ "-"*20)
            print(f"Current review: {int_begin_input+1}/{all_reviews_num}")

            if int_begin_input< 0 or int_begin_input > all_reviews_num -1:
                print("OUT OF BOUNDS!")
                break

            current_data = sampled_reviews.iloc[int_begin_input]
            int_begin_input += 1 # Pointing to next review
            store_url = restaurant_page_url(current_data["store_id"])
            print("Store Page URL: ", store_url)
            print(current_data) # Print series form of row

            print(f"You are labelling whether the review {labelled_column_name}")
            # Prompt for navigating reviews
            label_prompt = f"Enter 1 for {labelled_column_name}:True. Enter 0 for {labelled_column_name}:False."
            loop_prompt = "Enter 'q' to quit the program. Enter 'b' to move back to previous review"
            user_input = loop_prompt_until(label_prompt + "\n" + loop_prompt, lambda x: x in ['q','b', '0', '1'])
            if user_input == "q":
                break
            elif user_input == "b":
                int_begin_input -= 2 # It was pointing to next review, so must subtract 2 to point to previous review
            elif user_input in ['0', '1']:
                current_id = current_data[id_name]
                # Process input and update DB
                process_label_information(conn, 
                                          labelled_column_name=labelled_column_name,
                                          labelled_table_name=labelled_table_name,
                                          review_id_column_name=id_name,
                                          review_id_value=current_id,
                                          user_input=user_input)
            

    print("PROGRAM TERMINATION")
#%%
if __name__ == "__main__":
    args = parse_arguments()
    main(review_type=args.review_type)