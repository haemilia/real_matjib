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
    parser.add_argument("-rs", "--resample",
                        action="store_true")
    parser.add_argument("-s", "--sample_size",
                        type=int)
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
    url = f'https://pcmap.place.naver.com/restaurant/{id}/review/visitor?reviewSort=recent'
    return url

# Read list of review ids that are from the sampled restaurants, 
# and store the information as a file
def read_sampled_reviews(conn:duckdb.DuckDBPyConnection,
                         sampled_restaurants:pd.DataFrame,
                         table_name:str,
                         restaurants_table_name:str,
                         date_column:str) -> pd.DataFrame:
    """
    Reads review data for sampled restaurants, including only columns from the original
    reviews table, and orders reviews by date (latest first) within each restaurant.

    Args:
        conn (duckdb.DuckDBPyConnection): The active DuckDB connection.
        sampled_restaurants (pd.DataFrame): DataFrame of sampled restaurant IDs.
        table_name (str): The name of the original reviews table (e.g., 'navermap_reviews', 'naverblog_reviews').
        restaurants_table_name (str): The name of the restaurants table (e.g., 'restaurants').
        date_column (str): The name of the column containing the review date/datetime information.

    Returns:
        pd.DataFrame: A DataFrame containing sampled reviews, sorted by store_id and then
                      by the specified date_column (latest first), with only original review table columns.
    """
    try:
        # Get a relation object for the reviews table to infer schema
        reviews_relation = conn.table(table_name)
        
        # Get the column names from the relation's schema
        original_review_columns_list = [col for col in reviews_relation.columns]
        
        if not original_review_columns_list:
            raise ValueError(f"No columns found for table '{table_name}'. Does the table exist or is it empty?")
        
        # Ensure 'store_id' and the specified date_column are in the list for sorting
        if 'store_id' not in original_review_columns_list:
            raise ValueError(f"Column 'store_id' not found in table '{table_name}'. It is required for sorting by restaurant.")
        if date_column not in original_review_columns_list:
            raise ValueError(f"Column '{date_column}' not found in table '{table_name}'. It is required for sorting reviews by date.")
        
        # Convert the list of column names into a comma-separated string for the SELECT statement
        columns_to_select = ", ".join([f"r.{col}" for col in original_review_columns_list])
        
    except duckdb.Error as e:
        print(f"Error getting columns for table '{table_name}': {e}")
        raise # Re-raise the exception as this is a critical failure

    # Prepare sampled restaurant IDs for the IN clause
    # Ensure it's a tuple, which is suitable for DuckDB's parameterized IN clause
    sampled_restaurant_ids = tuple(sampled_restaurants["naver_store_id"].values)

    # Construct the main query using the dynamically obtained column names
    q0 = f"""SELECT {columns_to_select}
             FROM {table_name} AS r
             JOIN {restaurants_table_name} AS rest
             ON r.store_id = rest.naver_store_id
             WHERE rest.naver_store_id IN {sampled_restaurant_ids};
         """
    
    print(f"Executing query to read sampled reviews with selected columns:\n{q0}")
    
    # Execute the query and fetch into a DataFrame
    sampled_reviews = conn.execute(q0).fetchdf()

    if not sampled_reviews.empty:
        # Convert the specified date_column to datetime objects for correct sorting
        if date_column in sampled_reviews.columns:
            sampled_reviews[date_column] = pd.to_datetime(sampled_reviews[date_column])
        
        # Sort by store_id and then by the date_column (latest first)
        print("Sorting sampled reviews by restaurant and review date (latest first)...")
        sampled_reviews = sampled_reviews.sort_values(
            by=['store_id', date_column],
            ascending=[True, False] # Sort store_id ascending, date_column descending
        ).reset_index(drop=True) # Reset index after sorting for a clean DataFrame
    else:
        raise ValueError("No sampled reviews found to sort.")

    return sampled_reviews

# Create and prepare table for labelled data
def prepare_labelled_reviews_table(conn:duckdb.DuckDBPyConnection,
                                   sampled_reviews_df:pd.DataFrame,
                                   labelled_table_name:str,
                                   labelled_column_name:str):
    # Clean any past data
    q0 = f"DROP TABLE IF EXISTS {labelled_table_name};"
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
def we_need_sampling(conn: duckdb.DuckDBPyConnection, 
                     sampled_table_name: str) -> bool:
    """
    Checks if a sample table needs to be created or re-populated.
    Returns True if the table does not exist or exists but is empty.
    
    Args:
        conn (duckdb.DuckDBPyConnection): The active DuckDB connection.
        sampled_table_name (str): The name of the table to check for existence and data.
        
    Returns:
        bool: True if sampling is needed (table doesn't exist or is empty), False otherwise.
    """
    try:
        # Check 1: Does the table exist?
        # PRAGMA table_info returns an empty DataFrame if the table does not exist.
        table_exists_query = f"PRAGMA table_info('{sampled_table_name}');"
        table_info = conn.execute(table_exists_query).fetchdf()

        if table_info.empty:
            print(f"Table '{sampled_table_name}' does not exist. Sampling is needed.")
            return True # Table doesn't exist, so we need to sample.

        # Check 2: If the table exists, does it have any rows?
        row_count_query = f"SELECT COUNT(*) FROM {sampled_table_name};"
        result = conn.execute(row_count_query).fetchone()
        
        # `fetchone()` returns a tuple (count,) or None if query fails to return rows (unlikely for COUNT).
        if result is not None and result[0] > 0:
            row_count = result[0]
            print(f"Table '{sampled_table_name}' exists with {row_count} rows. Sampling is NOT needed.")
            return False # Table exists and has data, so no sampling needed.
        else:
            # Table exists but has 0 rows.
            print(f"Table '{sampled_table_name}' exists but is empty (0 rows). Sampling is needed.")
            return True # Table exists but is empty, so we need to sample.

    except duckdb.Error as e:
        # Catch any DuckDB-specific errors during the checks (e.g., malformed table name,
        # issues if the database file is corrupted, or a connection problem).
        print(f"DuckDB error while checking table '{sampled_table_name}': {e}")
        # If an error occurs, it's safer to assume sampling is needed, as we can't
        # reliably determine the state of the existing table.
        return True
    except Exception as e:
        # Catch any other unexpected Python errors.
        print(f"An unexpected error occurred while checking table '{sampled_table_name}': {e}")
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

# Save table in DB into a parquet file
def save_table_to_parquet(conn: duckdb.DuckDBPyConnection, table_name: str, destination_directory: Path):
    """
    Saves a table from a DuckDB database into a Parquet file.

    Args:
        conn: The DuckDB connection object.
        table_name: The name of the table to save.
        destination_directory: The Path object for the directory where the Parquet file will be saved.
    """
    destination_directory.mkdir(parents=True, exist_ok=True)

    output_filepath = destination_directory / f"{table_name}.parquet"

    try:
        # DuckDB's COPY command expects a string for the file path, so we convert the Path object to a string.
        conn.execute(f"COPY {table_name} TO '{output_filepath.as_posix()}' (FORMAT PARQUET);")
        print(f"Table '{table_name}' successfully saved to '{output_filepath}'")
    except duckdb.Error as e:
        print(f"Error saving table '{table_name}' to Parquet: {e}")



# Simple input UI loop:
#   - When going into a new restaurant, say that we're doing so, and display the URL
#   - Show information about the review (text, date, author username)
def main(db_path=Path(__file__).parent / ".." / "dataset" / "reviews.db",
         restaurants_table_name= "restaurants",
         review_type:str= "map",
         labelled_column_name:str="is_advert",
         resample = False,
         sample_size = 30):
    
    if review_type in ["map", "navermap_reviews"]:
        table_name = "navermap_reviews"
        id_name = "review_id"
        date_column_name = "review_datetime"
    elif review_type in ["blog", "naverblog_reviews"]:
        table_name = "naverblog_reviews"
        id_name = "post_id"
        date_column_name = ""
    else:
        assert review_type in ["map", "blog", "navermap_reviews", "naverblog_reviews"]
        return
    # Announce beginning of program
    print("--- LABELLER ---")
    print("LOADING..." + "\n"*3)
    # DB connection initialisation
    with duckdb.connect(str(db_path)) as conn:
        labelled_table_name = f"{table_name}_labelled"

        if resample:
            needs_new_sample = True
        else:
            needs_new_sample = we_need_sampling(conn, labelled_table_name)

        if needs_new_sample:
            print("Reading tables from DB...")
            # Read restaurants
            restaurants = read_restaurants_from_db(conn)
            print("Sampling...")
            # Sample restaurants
            sampled_restaurants = sample_restaurants(restaurants,sample_size=sample_size)
            # Get sampled reviews
            sampled_reviews = read_sampled_reviews(conn, 
                                                   sampled_restaurants, 
                                                   table_name, 
                                                   restaurants_table_name,
                                                   date_column_name)
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
            initial_start_index = unlabelled_reviews.index[0]
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
            # Retrieve data from that index
            current_data = sampled_reviews.iloc[int_begin_input]
            current_store_id = current_data["store_id"]
            num_reviews_for_current_store = len(sampled_reviews[sampled_reviews["store_id"] == current_store_id])
            print(f"Store ID: {current_store_id} | Reviews for this store in sample: {num_reviews_for_current_store}")
            
            # Retrieve the current label for the displayed review
            current_label_value = current_data.get(labelled_column_name)
            if pd.isna(current_label_value):
                print("\nThis review has not been labeled yet.")
                prompt_options = ['q', 'b', '0', '1', 's']
                extra_prompt_text = ""
            else:
                label_display = "True" if current_label_value else "False"
                print(f"\nPreviously labeled as: {labelled_column_name}: {label_display}")
                prompt_options = ['q', 'b', 'n', '0', '1', "", 's'] # Add 'n' option (and equivalent "" option)
                extra_prompt_text = "Enter 'n', or nothing to go to the next entry without changing the label. "
            
            store_url = restaurant_page_url(current_data["store_id"])
            print("Store Page URL: ", store_url)
            print()
            print(current_data["review_text"])
            print()
            
            print(current_data) # Print series form of row

            print(f"You are labelling whether the review {labelled_column_name}")
            # Prompt for navigating reviews
            label_prompt = f"Enter 1 for {labelled_column_name}:True. Enter 0 for {labelled_column_name}:False."
            loop_prompt = extra_prompt_text + "Enter 'q' to quit the program. Enter 'b' to move back to previous review: "
            user_input = loop_prompt_until(label_prompt + "\n" + loop_prompt, lambda x: x in prompt_options)
            if user_input == "q":
                break
            elif user_input == "b":
                int_begin_input -= 1 # Point to previous review
            elif user_input in ["n", ""]:
                int_begin_input += 1 # Point to next review
            elif user_input in ['0', '1']:
                current_id = current_data[id_name]
                # Process input and update DB
                process_label_information(conn, 
                                          labelled_column_name=labelled_column_name,
                                          labelled_table_name=labelled_table_name,
                                          review_id_column_name=id_name,
                                          review_id_value=current_id,
                                          user_input=user_input)
                
                int_begin_input += 1 # Pointing to next review
            elif user_input == "s":
                save_table_to_parquet(conn, labelled_table_name, Path("G:/My Drive/Data/naver_search_results/"))
            
            

    print("PROGRAM TERMINATION")
#%%
if __name__ == "__main__":
    args = parse_arguments()
    main(review_type=args.review_type, 
         resample=args.resample, 
         sample_size=args.sample_size,)
# %%
