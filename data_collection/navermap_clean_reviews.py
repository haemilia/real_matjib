# Check out the collected data
#%%
import pandas as pd
import numpy as np
from pathlib import Path
import pickle
import json

OUTPUT_DIR = Path('G:/My Drive/Data/naver_search_results')
OUTPUT_DIR.mkdir(exist_ok=True)

with open(OUTPUT_DIR /"mapogu_yeonnamdong_naver.json", "r") as f:
    restaurants = json.load(f)

id_to_name = {}
for restaurant, content in restaurants.items():
    if len(content) == 0:
        continue
    for one_store in content:
        store_id = one_store["id"]
        id_to_name[store_id] = restaurant
id_to_name_path = OUTPUT_DIR / "restaurant_id_to_name.pkl"
with open(id_to_name_path, "wb") as wf:
    pickle.dump(id_to_name, wf)


with open(OUTPUT_DIR/"mapogu_yeonnamdong_naver_reviews_final.pkl", "rb") as rf:
    reviews = pickle.load(rf)

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
# %%
navermap_reviews_path = Path("../dataset/navermap_reviews.parquet.gzip")
navermap_reviews.to_parquet(navermap_reviews_path, compression="gzip")