# Check out the collected data
#%%
import pandas as pd
from pathlib import Path
import pickle
import json

def get_restaurants(restaurants_path:Path) -> dict:
    with open(restaurants_path, "r") as f:
        restaurants = json.load(f)
    return restaurants

def get_reviews(reviews_path:Path) -> dict:
    with open(reviews_path, "rb") as rf:
        reviews = pickle.load(rf)
    return reviews

def create_id_to_name(restaurants)-> dict:
    id_to_name = {}
    for restaurant, content in restaurants.items():
        if len(content) == 0:
            continue
        for one_store in content:
            store_id = one_store["id"]
            id_to_name[store_id] = restaurant
    return id_to_name

def tabularise_navermap_reviews(restaurants:dict, reviews:dict) -> pd.DataFrame:
    # Create id_to_name dict 
    # To reference later 
    id_to_name = create_id_to_name(restaurants)

    all_rows = []
    for current_id, one_store in reviews.items():
        for review in one_store:
            if not review.get("id", False):
                continue # It should have a review id in the very least. Assume faulty data if it doesn't have it.
            try:
                row = {}
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
    return tag_list
    
def parse_keyword_tags_hangul(keyword_tags):
    tag_list = []
    for tag in keyword_tags:
        tag_list.append(tag.get("name"))
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
    return image_links
def parse_video_thumbnail_links(review_images):
    # Can't access video with only video url
    video_links = []
    for asset in review_images:
        if not isinstance(asset, dict):
            continue
        if asset.get("type") == "video":
            video_links.append(asset.get("thumbnail"))
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
    return kw_list
leave_as_it_is = lambda x: x
#######################################################################################################
def get_cleansing()-> dict:
    cleansing = {"purchase_item": parse_purchase_item,
             "store_id": leave_as_it_is,
             "store_naver_name": leave_as_it_is,
             "store_name":leave_as_it_is,
             "review_id": leave_as_it_is,
             "review_text": leave_as_it_is,
             "image_links": parse_image_links,
             "num_of_media": parse_num_of_media,
             "video_thumbnail_links": parse_video_thumbnail_links,
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
             }
    return cleansing

def cleanse_navermap_reviews(navermap_reviews:pd.DataFrame, cleansing:dict)-> pd.DataFrame:
    new_df = {}
    new_df["review_id"] = navermap_reviews["review_id"].apply(cleansing["review_id"])
    new_df["store_id"] = navermap_reviews["store_id"].apply(cleansing["store_id"])
    new_df["store_naver_name"] = navermap_reviews["store_naver_name"].apply(cleansing["store_naver_name"])
    new_df["store_name"] = navermap_reviews["store_name"].apply(cleansing["store_name"])
    new_df["review_text"] = navermap_reviews["review_text"].apply(cleansing["review_text"])
    new_df["num_of_media"] = navermap_reviews["review_images"].apply(cleansing["num_of_media"])
    new_df["image_links"] = navermap_reviews["review_images"].apply(cleansing["image_links"])
    new_df["video_thumbnail_links"] = navermap_reviews["review_images"].apply(cleansing["video_thumbnail_links"])
    new_df["reactions_fun"] = navermap_reviews["reactions"].apply(cleansing["reactions_fun"])
    new_df["reactions_helpful"] = navermap_reviews["reactions"].apply(cleansing["reactions_helpful"])
    new_df["reactions_wannago"] = navermap_reviews["reactions"].apply(cleansing["reactions_wannago"])
    new_df["reactions_cool"] = navermap_reviews["reactions"].apply(cleansing["reactions_cool"])
    new_df["review_datetime"] = navermap_reviews["review_datetime"].apply(cleansing["review_datetime"])
    new_df["review_year"] = navermap_reviews["review_datetime"].apply(cleansing["review_year"])
    new_df["visit_keywords"] = navermap_reviews["visit_keywords"].apply(cleansing["visit_keywords"])
    new_df["rating"] = navermap_reviews["rating"].apply(cleansing["rating"])
    new_df["keyword_tags_code"] = navermap_reviews["keyword_tags"].apply(cleansing["keyword_tags_code"])
    new_df["keyword_tags_hangul"] = navermap_reviews["keyword_tags"].apply(cleansing["keyword_tags_hangul"])
    new_df_pd = pd.DataFrame(new_df)
    return new_df_pd

#################################################### MAIN ##################################################################
if __name__ == "__main__":

    backup_storage_dir = Path('G:/My Drive/Data/naver_search_results')

    # Get restaurants data
    print("Getting restaurant data...")
    restaurants_path:Path = backup_storage_dir / "mapogu_yeonnamdong_naver.json"
    restaurants:dict = get_restaurants(restaurants_path)
    # Get reviews data
    print("Getting reviews data...")
    reviews_path:Path = backup_storage_dir / "mapogu_yeonnamdong_naver_reviews_final.pkl"
    reviews:dict = get_reviews(reviews_path)

    # Tabularise reviews
    print("Tabularise reviews...")
    navermap_reviews:pd.DataFrame = tabularise_navermap_reviews(restaurants, reviews)

    # Cleanse reviews
    print("Cleanse reviews...")
    cleansing:dict = get_cleansing()
    navermap_reviews_final:pd.DataFrame = cleanse_navermap_reviews(navermap_reviews, cleansing)

    # Save navermap_reviews_final
    dataset_dir:Path = Path("../dataset")
    navermap_reviews_final_path:Path = dataset_dir / "navermap_reviews_final.parquet.gzip"
    print(f"Saving navermap reviews at {navermap_reviews_final_path}...")
    navermap_reviews_final.to_parquet(navermap_reviews_final_path, compression="gzip")

# %%
