#%%
import pandas as pd
from pathlib import Path
import pickle
import json
from typing import Dict, Callable
#### Restaurants Raw
def get_restaurants_raw(restaurants_raw_path:Path, focus_region:str) -> pd.DataFrame:
    raw_df = pd.read_excel(restaurants_raw_path)
    with_address = raw_df.dropna(subset=["소재지전체주소", "도로명전체주소"])
    region_df = with_address[with_address["도로명전체주소"].str.contains(focus_region)]
    interested_df = region_df[["사업장명", "좌표정보X(EPSG5174)", '좌표정보Y(EPSG5174)', "소재지전체주소", "도로명전체주소"]]
    interested_df = interested_df.dropna(how="all", axis=1).copy()
    result_df = interested_df.rename(columns = {"사업장명": "store_name",
                                            "좌표정보X(EPSG5174)": "X_EPSG_5174",
                                            '좌표정보Y(EPSG5174)': "Y_EPSG_5174",
                                            "소재지전체주소": "jibun_address",
                                            "도로명전체주소": "road_address"})
    result_df.drop_duplicates(subset="store_name", inplace=True)
    return result_df


#### Restaurants 
def get_restaurants(restaurants_path:Path, is_pickle=False) -> dict:
    if is_pickle:
        with open(restaurants_path, "rb") as rf:
            restaurants = pickle.load(rf)
    else:
        with open(restaurants_path, "r", encoding="utf-8") as f:
            restaurants = json.load(f)
    return restaurants

def create_id_to_name(restaurants)-> dict:
    id_to_name = {}
    for restaurant, content in restaurants.items():
        if len(content) == 0:
            continue
        for one_store in content:
            store_id = one_store["id"]
            id_to_name[store_id] = restaurant
    return id_to_name

def get_food_categories(food_categories_path:Path) -> list:
    with open(food_categories_path, "r", encoding="utf-8") as f:
        food_categories = json.load(f)
    return food_categories

def category_is_food(checking_cat:list, food_category_list:list) -> bool:
    result = []
    for cat in checking_cat:
        if (cat in food_category_list) or ("음식" in checking_cat):
            result.append(True)
        else:
            result.append(False)
    return any(result)


#### Ultimate Naver Restaurants Table
def jaccard_similarity(word1:str, word2:str)->float:
    """
    Calculates the Jaccard similarity between two words based on their letter sets.

    Args:
        word1 (str): The first word.
        word2 (str): The second word.

    Returns:
        float: The Jaccard similarity between the two words (between 0.0 and 1.0).
               Returns 0.0 if either word is empty.
    """
    if not word1 or not word2:
        return 0.0

    set1 = set(word1)
    set2 = set(word2)

    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

def drop_duplicates_by_similarity(df:pd.DataFrame, 
                                  duplicate_column:str, 
                                  similarity_column1:str, 
                                  similarity_column2:str) -> pd.Series|pd.DataFrame:
    """
    Drops duplicate rows based on a specified column, keeping the row with the
    highest Jaccard similarity between the values in two other specified columns.

    Args:
        df (pd.DataFrame): The input DataFrame.
        duplicate_column (str): The name of the column to identify duplicates.
        similarity_column1 (str): The name of the first column to use for Jaccard
                                    similarity comparison.
        similarity_column2 (str): The name of the second column to use for Jaccard
                                    similarity comparison.

    Returns:
        pd.DataFrame: A new DataFrame with duplicates dropped.
    """
    def keep_best(group):
        if len(group) == 1:
            return group
        similarities = group.apply(lambda row: jaccard_similarity(row[similarity_column1], row[similarity_column2]), axis=1)
        best_index = similarities.idxmax()
        return group.loc[[best_index]]

    result = df.groupby(duplicate_column, group_keys=False).apply(keep_best).reset_index(drop=True)

    return result

def tabularise_navermap_restaurants(restaurants:dict, 
                                    restaurants_raw:pd.DataFrame, 
                                    filter_restaurants=True, 
                                    food_categories_path= Path("../dataset/naver_food_categories.json")) -> pd.DataFrame|pd.Series:
    if filter_restaurants: # If the option is to filter for restaurants, 
        # load food_categories_list to compare with
        # to only maintain stores with categories related to food.
        food_categories_list = get_food_categories(food_categories_path)
    else:
        food_categories_list = []
    # Container for rows of the final table   
    rows = []
    for store_name, store_content in restaurants.items():
        store_content_doesnt_exist = store_content is None # if there's no content, we can skip
        if store_content_doesnt_exist or len(store_content) == 0: # if there's no elements in the list, we can skip
            continue
        for store in store_content:
            if filter_restaurants and not category_is_food(store.get("category"), food_categories_list):
                continue
            row = {}
            row["naver_store_id"] = store.get("id")
            row["store_name"] = store_name
            row["naver_store_name"] = store.get("name")
            row["category"] = store.get("category")
            row["naver_jibun_address"] = store.get("address")
            row["naver_road_address"] = store.get("roadAddress")
            row["naver_blog_review_count"] = store.get("reviewCount")
            row["naver_place_review_count"] = store.get("placeReviewCount")
            row["X_naver_WGS_84"] = store.get("x")
            row["Y_naver_WGS_84"] = store.get("y")
            rows.append(row)
    restaurants_naver = pd.DataFrame(rows) # Construct dataframe from rows
    # Merge with restaurants_raw df
    restaurants_table = pd.merge(restaurants_naver, restaurants_raw, on="store_name", how="left")
    # Filter for duplicate values; Keep only the rows with high similarity between store names
    final_restaurants_table = drop_duplicates_by_similarity(restaurants_table, "naver_store_id", "naver_store_name", "store_name")
    return final_restaurants_table

#### Reviews 
def get_reviews(reviews_path:Path) -> dict:
    with open(reviews_path, "rb") as rf:
        reviews = pickle.load(rf)
    return reviews

def tabularise_navermap_reviews(restaurants:dict, reviews:dict) -> pd.DataFrame:
    # Create id_to_name dict 
    # To reference later 
    id_to_name = create_id_to_name(restaurants)

    all_rows = []
    for current_id, one_store in reviews.items():
        for review in one_store:
            if not review.get("id", False):
                continue # It should have a review id in the very least. Assume faulty data if it doesn't have it.
            row = {}
            try:
                row["store_id"] = current_id
                row["store_naver_name"] = review.get("businessName")
                row["store_name"] = id_to_name[current_id]
                row["review_id"] = review["id"]
                row["author_id"] = review.get("author", {}).get("id")
                row["author_nickname"] = review.get("author", {}).get("nickname")
                if review.get("author", {}).get("review") is not None:
                    row["author_total_reviews"] = review.get("author", {}).get("review", {}).get("totalCount")
                    row["author_total_images"] = review.get("author", {}).get("review", {}).get("imageCount")
                else:
                    row["author_total_reviews"] = None
                    row["author_total_images"] = None
                row["rating"] = review.get("rating")
                row["author_page_url"] = review.get("author", {}).get("url")
                row["review_text"] = review.get("body")
                row["review_images"] = review.get("media", [])
                row["visit_count"] = review.get("visitCount")
                row["review_view_count"] = review.get("viewCount")
                row["store_reply"] = review.get("reply", {}).get("body")
                row["review_type"] = review.get("originType") # 영수증 or 결제 내역
                row["purchase_item"] = review.get("item")
                row["keyword_tags"] = review.get("votedKeywords")
                row["reactions"] = review.get("reactionStat", {}).get("typeCount", [])
                row["visit_keywords"] = review.get("visitCategories", [])
                row["review_datetime"] = review.get("representativeVisitDateTime")
            except Exception as e:
                print("There was an error:", e)
                print(json.dumps(review, indent=4, ensure_ascii=False))

            all_rows.append(row)
    navermap_reviews = pd.DataFrame(all_rows)
    return navermap_reviews

#################### Defining functions for cleansing reviews ##################################################
def parse_purchase_item(purchase_item):
    if purchase_item is None:
        return None
    if isinstance(purchase_item, dict):
        return purchase_item.get("name", None)
    else:
        raise TypeError("Something else was here!")
def parse_keyword_tags_code(keyword_tags):
    tag_list = []
    for tag in keyword_tags:
        tag_list.append(tag.get("code"))
    if len(tag_list) == 0:
        return None
    return tag_list
    
def parse_keyword_tags_hangul(keyword_tags):
    tag_list = []
    for tag in keyword_tags:
        tag_list.append(tag.get("name"))
    if len(tag_list) == 0:
        return None
    return tag_list
def parse_reactions_fun(reactions):
    count = None
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        if reaction.get('name') == "fun":
            count = reaction.get("count")
    return count
def parse_reactions_helpful(reactions):
    count = None
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        if reaction.get('name') == "helpful":
            count = reaction.get("count")
    return count
def parse_reactions_wannago(reactions):
    count = None
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        if reaction.get('name') == "wannago":
            count = reaction.get("count")
    return count
def parse_reactions_cool(reactions):
    count = None
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        if reaction.get('name') == "cool":
            count = reaction.get("count")
    return count
def parse_num_of_media(review_images):
    return len(review_images)
def parse_image_links(review_images):
    image_links = []
    for asset in review_images:
        if not isinstance(asset, dict):
            continue
        if asset.get("type") == 'image':
            image_links.append(asset.get("thumbnail"))
    if len(image_links) == 0:
        return None
    return image_links
def parse_video_thumbnail_links(review_images):
    # Can't access video with only video url
    video_links = []
    for asset in review_images:
        if not isinstance(asset, dict):
            continue
        if asset.get("type") == "video":
            video_links.append(asset.get("thumbnail"))
    if len(video_links) == 0:
        return None
    return video_links
def transform_old_year_modulo(dt):
    current_century_start = 2000
    if dt.year < 1900:
        new_year = current_century_start + (dt.year % 100)
        return pd.Timestamp(year=new_year, month=dt.month, day=dt.day,
                            hour=dt.hour, minute=dt.minute, second=dt.second,
                            nanosecond=dt.nanosecond)
    return dt
def parse_review_datetime(review_datetime):
    if review_datetime is None:
        return pd.NaT
    into_timestamp = transform_old_year_modulo(pd.Timestamp(review_datetime).tz_localize(None))
    return into_timestamp
def parse_review_year(review_datetime):
    if review_datetime is None:
        return pd.NaT
    into_timestamp = pd.Timestamp(review_datetime).tz_localize(None)
    return into_timestamp.year
def parse_visit_keywords(visit_keywords):
    kw_list = []
    for keyword in visit_keywords:
        if not isinstance(keyword, dict):
            continue
        for ii in keyword.get("keywords", []):
            kw_list.append(ii.get("name"))
    if len(kw_list) == 0:
        return None
    return kw_list
def leave_as_it_is(x):
    return x
def parse_visit_count(visit_count):
    if isinstance(visit_count, int):
        return visit_count
    elif isinstance(visit_count, str) and visit_count.isdigit():
        return int(visit_count)
    else:
        return None
#######################################################################################################
def get_cleansing()-> Dict[str, Callable]:
    cleansing = {"purchase_item": parse_purchase_item,
             "store_id": leave_as_it_is,
             "store_naver_name": leave_as_it_is,
             "store_name":leave_as_it_is,
             "review_id": leave_as_it_is,
             "review_text": leave_as_it_is,
             "image_links": parse_image_links,
             "num_of_media": parse_num_of_media,
             "video_thumbnail_links": parse_video_thumbnail_links,
             "author_nickname": leave_as_it_is,
             "author_total_reviews":leave_as_it_is,
             "author_total_images": leave_as_it_is,
             "reactions_fun": parse_reactions_fun,
             "reactions_helpful": parse_reactions_helpful,
             "reactions_wannago": parse_reactions_wannago,
             "reactions_cool": parse_reactions_cool,
             "review_datetime":parse_review_datetime,
             "review_year": parse_review_year, # Might be useful for filtering?
             "visit_keywords": parse_visit_keywords,
             "rating": leave_as_it_is,
             "keyword_tags_code": parse_keyword_tags_code,
             "keyword_tags_hangul": parse_keyword_tags_hangul,
             "visit_count": parse_visit_count,
             "store_reply": leave_as_it_is,
             }
    return cleansing

def cleanse_navermap_reviews(navermap_reviews:pd.DataFrame, cleansing:Dict[str, Callable])-> pd.DataFrame:
    new_df = {}
    new_df["review_id"] = navermap_reviews["review_id"].apply(cleansing["review_id"])
    new_df["store_id"] = navermap_reviews["store_id"].apply(cleansing["store_id"])
    new_df["store_naver_name"] = navermap_reviews["store_naver_name"].apply(cleansing["store_naver_name"])
    new_df["store_name"] = navermap_reviews["store_name"].apply(cleansing["store_name"])
    new_df["store_reply"] = navermap_reviews["store_reply"].apply(cleansing["store_reply"])
    new_df["review_text"] = navermap_reviews["review_text"].apply(cleansing["review_text"])
    new_df["num_of_media"] = navermap_reviews["review_images"].apply(cleansing["num_of_media"])
    new_df["image_links"] = navermap_reviews["review_images"].apply(cleansing["image_links"])
    new_df["video_thumbnail_links"] = navermap_reviews["review_images"].apply(cleansing["video_thumbnail_links"])
    new_df["visit_count"] = navermap_reviews["visit_count"].apply(cleansing["visit_count"])
    new_df["author_nickname"] = navermap_reviews["author_nickname"].apply(cleansing["author_nickname"])
    new_df["author_total_reviews"] = navermap_reviews["author_total_reviews"].apply(cleansing["author_total_reviews"])
    new_df["author_total_images"] = navermap_reviews["author_total_images"].apply(cleansing["author_total_images"])
    new_df["reactions_fun"] = navermap_reviews["reactions"].apply(cleansing["reactions_fun"])
    new_df["reactions_helpful"] = navermap_reviews["reactions"].apply(cleansing["reactions_helpful"])
    new_df["reactions_wannago"] = navermap_reviews["reactions"].apply(cleansing["reactions_wannago"])
    new_df["reactions_cool"] = navermap_reviews["reactions"].apply(cleansing["reactions_cool"])
    new_df["review_datetime"] = navermap_reviews["review_datetime"].apply(cleansing["review_datetime"])
    new_df["review_year"] = navermap_reviews["review_datetime"].apply(cleansing["review_year"])
    new_df["visit_keywords"] = navermap_reviews["visit_keywords"].apply(cleansing["visit_keywords"])
    new_df["purchase_item"] = navermap_reviews["purchase_item"].apply(cleansing["purchase_item"])
    new_df["rating"] = navermap_reviews["rating"].apply(cleansing["rating"])
    new_df["keyword_tags_code"] = navermap_reviews["keyword_tags"].apply(cleansing["keyword_tags_code"])
    new_df["keyword_tags_hangul"] = navermap_reviews["keyword_tags"].apply(cleansing["keyword_tags_hangul"])
    new_df_pd = pd.DataFrame(new_df)
    new_df_pd.drop_duplicates(subset="review_id", inplace=True)
    return new_df_pd

#################################################### MAIN ##################################################################
def main(restaurants_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver.json"),
         restaurants_raw_path=Path("../dataset/seoul_mapogu_general_restaurants.xlsx"),
         food_categories_path = Path("../dataset/naver_food_categories.json"),
         reviews_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_reviews_final.pkl"),
         navermap_reviews_final_path=Path("../dataset/navermap_reviews_final.parquet.gzip"),
         navermap_reviews_final_backup_path=Path("G:/My Drive/Data/naver_search_results/navermap_reviews_final.parquet.gzip"),
         restaurants_table_path = Path("../dataset/restaurants_table.parquet"),
         restaurants_table_backup_path = Path("G:/My Drive/Data/naver_search_results/restaurants_table.parquet"),
         REGION_OF_FOCUS="연남동"):
    
    # BACKUP_STORAGE_DIR = Path('G:/My Drive/Data/naver_search_results')
    # DATASET_DIR = Path("../dataset")

    # Get raw restaurants data
    print("Getting original restaurant data")
    restaurants_raw = get_restaurants_raw(restaurants_raw_path, REGION_OF_FOCUS)

    # Get restaurants data
    print("Getting restaurant data...")
    # restaurants_path:Path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver.json"
    restaurants = get_restaurants(restaurants_path)

    # Create restaurants table
    print("Creating restaurants table...")
    restaurants_table = tabularise_navermap_restaurants(restaurants, restaurants_raw, 
                                                        filter_restaurants=True,
                                                        food_categories_path=food_categories_path)

    # Get reviews data
    print("Getting reviews data...")
    # reviews_path:Path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver_reviews_final.pkl"
    reviews = get_reviews(reviews_path)

    # Tabularise reviews
    print("Tabularise reviews...")
    navermap_reviews = tabularise_navermap_reviews(restaurants, reviews)

    # Cleanse reviews
    print("Cleanse reviews...")
    cleansing = get_cleansing()
    navermap_reviews_final = cleanse_navermap_reviews(navermap_reviews, cleansing)

    # Save restaurants table
    print(f"Saving restaurants table at {restaurants_table_path}")
    restaurants_table.to_parquet(restaurants_table_path)
    restaurants_table.to_parquet(restaurants_table_backup_path)

    # Save navermap_reviews_final
    # navermap_reviews_final_path:Path = DATASET_DIR / "navermap_reviews_final.parquet.gzip"
    # navermap_reviews_final_backup_path:Path = BACKUP_STORAGE_DIR / "navermap_reviews_final.parquet.gzip"
    print(f"Saving navermap reviews at {navermap_reviews_final_path}...")
    navermap_reviews_final.to_parquet(navermap_reviews_final_path, compression="gzip")
    navermap_reviews_final.to_parquet(navermap_reviews_final_backup_path, compression="gzip")


#%%
if __name__ == "__main__":
    main()

    # backup_storage_dir = Path('G:/My Drive/Data/naver_search_results')

    # # Get restaurants data
    # print("Getting restaurant data...")
    # restaurants_path:Path = backup_storage_dir / "mapogu_yeonnamdong_naver.json"
    # restaurants:dict = get_restaurants(restaurants_path)
    # # Get reviews data
    # print("Getting reviews data...")
    # reviews_path:Path = backup_storage_dir / "mapogu_yeonnamdong_naver_reviews_final.pkl"
    # reviews:dict = get_reviews(reviews_path)

    # # Tabularise reviews
    # print("Tabularise reviews...")
    # navermap_reviews:pd.DataFrame = tabularise_navermap_reviews(restaurants, reviews)

    # # Cleanse reviews
    # print("Cleanse reviews...")
    # cleansing:dict = get_cleansing()
    # navermap_reviews_final:pd.DataFrame = cleanse_navermap_reviews(navermap_reviews, cleansing)

    # # Save navermap_reviews_final
    # dataset_dir:Path = Path("../dataset")
    # navermap_reviews_final_path:Path = dataset_dir / "navermap_reviews_final.parquet.gzip"
    # navermap_reviews_final_backup_path:Path = backup_storage_dir / "navermap_reviews_final.parquet.gzip"
    # print(f"Saving navermap reviews at {navermap_reviews_final_path}...")
    # navermap_reviews_final.to_parquet(navermap_reviews_final_path, compression="gzip")
    # navermap_reviews_final.to_parquet(navermap_reviews_final_backup_path, compression="gzip")

#%%
# from pathlib import Path
# import pickle
# BACKUP_STORAGE_DIR= Path('G:/My Drive/Data/naver_search_results')
# restaurants_path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver_.pkl"
# with open(restaurants_path, "rb") as rf:
#     restaurants = pickle.load(rf)

# restaurants_raw_path = Path("../dataset/seoul_mapogu_general_restaurants.xlsx")
# rraw = get_restaurants_raw(restaurants_raw_path, focus_region="연남동")

# restaurants_table = tabularise_navermap_restaurants(restaurants, 
#                                                     rraw, 
#                                                     filter_restaurants=True,
#                                                     food_categories_path=Path("../dataset/naver_food_categories.json"))
#%%

# restaurants_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver.json")
# restaurants_raw_path=Path("../dataset/seoul_mapogu_general_restaurants.xlsx")
# food_categories_path = Path("../dataset/naver_food_categories.json")
# reviews_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_reviews_final.pkl")
# navermap_reviews_final_path=Path("../dataset/navermap_reviews_final.parquet.gzip")
# navermap_reviews_final_backup_path=Path("G:/My Drive/Data/naver_search_results/navermap_reviews_final.parquet.gzip")
# restaurants_table_path = Path("../dataset/restaurants_table.parquet")
# restaurants_table_backup_path = Path("G:/My Drive/Data/naver_search_results/restaurants_table.parquet")

# reviews_table = pd.read_parquet(navermap_reviews_final_backup_path)
# reviews_table