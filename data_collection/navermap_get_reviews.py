#%%
import requests
import json
import pickle
from pathlib import Path
from math import ceil
from time import sleep
from tqdm import tqdm

def post_request_for_naver_place_reviews(restaurant_id, cid_list, page_num):
    url = "https://api.place.naver.com/graphql"
    visitor_reviews_query = """
    query getVisitorReviews($input: VisitorReviewsInput) {
    visitorReviews(input: $input) {
    items {
    id
    reviewId
    rating
    author {
        id
        nickname
        from
        imageUrl
        borderImageUrl
        objectId
        url
        review {
        totalCount
        imageCount
        avgRating
        __typename
        }
        theme {
        totalCount
        __typename
        }
        isFollowing
        followerCount
        followRequested
        __typename
    }
    body
    thumbnail
    media {
        type
        thumbnail
        thumbnailRatio
        class
        videoId
        videoUrl
        trailerUrl
        __typename
    }
    tags
    status
    visitCount
    viewCount
    visited
    created
    reply {
        editUrl
        body
        editedBy
        created
        date
        replyTitle
        isReported
        isSuspended
        status
        __typename
    }
    originType
    item {
        name
        code
        options
        __typename
    }
    language
    highlightRanges {
        start
        end
        __typename
    }
    apolloCacheId
    translatedText
    businessName
    showBookingItemName
    bookingItemName
    votedKeywords {
        code
        iconUrl
        iconCode
        name
        __typename
    }
    userIdno
    loginIdno
    receiptInfoUrl
    reactionStat {
        id
        typeCount {
        name
        count
        __typename
        }
        totalCount
        __typename
    }
    showPaymentInfo
    visitCategories {
        code
        name
        keywords {
        code
        name
        __typename
        }
        __typename
    }
    representativeVisitDateTime
    showRepresentativeVisitDateTime
    __typename
    }
    starDistribution {
    score
    count
    __typename
    }
    hideProductSelectBox
    total
    showRecommendationSort
    itemReviewStats {
    score
    count
    itemId
    starDistribution {
        score
        count
        __typename
    }
    __typename
    }
    __typename
    }
    }
    """

    payload = [
        {
            "operationName": "getVisitorReviews",
            "variables": {
                "input": {
                    "businessId": restaurant_id,
                    "businessType": "restaurant",
                    "item": "0",
                    "bookingBusinessId": None,
                    "page": page_num,
                    "size": 10,
                    "isPhotoUsed": False,
                    "includeContent": True,
                    "getUserStats": True,
                    "includeReceiptPhotos": True,
                    "cidList": cid_list,
                    "getReactions": True,
                    "getTrailer": True,
                }
            },
            "query": visitor_reviews_query,
        }
    ]

    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'ko',
        'content-type': 'application/json',
        'cookie': 'NNB=NOQ5IPEEUZ6GO; _fbp=fb.1.1744598109520.283917362332682121; ASID=b76d75f40000019632869f8b00000023; nstore_session=xgVCpz13V/+fJLNAc1DreETT; _ga=GA1.1.1385620538.1736215668; _ga_EFBDNNF91G=GS1.1.1744608711.1.0.1744608712.0.0.0; NAC=4QvVBkAlIfW0B; nid_inf=1949580813; NID_AUT=kl5afNNCajodpaWvKT4D5zEtB/uChF7Wcq3ih3RamPJ/RC7Z2y9iXzzJfFfWARjl; nstore_pagesession=juKb2wqWLGk79lsM3pV-387156; SRT30=1747703571; SRT5=1747703571; NID_SES=AAABqrC252es89UgY2HAproLwPPzM5HmwJQasnceB3EOLGWL3SenbKikMtQiJUk0MEzpV80UPKEdKMkTurDMAO2sXNZVy7cMTKIUfvDTaw1pRTVeBs8RlOfeS//D52pPmlbFUBaNDvjhLznpkfZWo1X5gTO9M2JDIiJdzutuXxAUHlfxdWi51nmVCP7pxo2RvJ/gp8Cs7b2Nny+4D8UHui5Be8vJ85igB1b12D2+VOZNZ0ZPKOwXR7s+qiPwT+yFab8Hd7HgmgHe3OwO8gM3txqKvn1CJQnS+97AAnd6AqH6GiTlXG2mgvSZbPCLZz4MxpD+c0ALK2dkNA7GiVgEiAvKuVmudUVyK5dZ8FBvrQbhZyVPwSHzeQLHzs7dpEUpLEYEwDWjY4OzaLySzTVN7JzjEnGk3H2rmiletiaMufKoMQI+X/6FhqBG+N6A+HzCyz0oy2UJlTFAiOUyLdHal+doNxdvJIvKjyyP9WExxLDQ5ePCsKah4EEuLCJFlOpAsUF6BLVl57DVvrEEgGd0mgZrbj+AfxW/6Z7Z6Qbh9N6r5PSwnqWVMIZklZa6ZNql4fuhqg==; BUC=WwyaNTH_cxpadHXahyVHtlxazVAHPtk1DSsuaf1ogUI=',
        'origin': 'https://m.place.naver.com',
        'priority': 'u=1, i',
        'referer': f'https://m.place.naver.com/restaurant/{restaurant_id}/review/visitor',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'x-ncaptcha-violation': 'false',
        'x-wtm-graphql': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjYXJkIjp7ImJ1c2luZXNzSWQiOiIyMDM4MTc1NDUyIiwidHlwZSI6InJlc3RhdXJhbnQiLCJzb3VyY2UiOiJwbGFjZSJ9LCJpYXQiOjE3MTYxODU2MzksImV4cCI6MTcxNjE4NjIzOX0.YkRzZQk4c0qVzT1n_fSsvt84-44wWj1r8l4NRkY-aT4',
    }



    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()
        return data
        # print(json.dumps(data, indent=4, ensure_ascii=False))

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise e
    except json.JSONDecodeError as e:
        print("Failed to decode JSON response.")
        print("Response text:", response.text)
        raise e

def main(restaurants_path=Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver.json"),
         reviews_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_reviews_final.pkl"),
         reviews_json_path = Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_reviews.json")):
    # BACKUP_STORAGE_DIR= Path('G:/My Drive/Data/naver_search_results')
    # DATASET_DIR = Path("../dataset")

    # Get restaurants
    # restaurants_path:Path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver.json"
    with open(restaurants_path, "r", encoding="utf-8") as f:
        restaurants = json.load(f)
    
    # requested_restaurant: somewhere to mark whether I've requested the restaurant or not
    requested_restaurant_path = Path("../requested_restaurant.pkl")
    if requested_restaurant_path.exists(): # If it already exists, read it
        with open(requested_restaurant_path, "rb") as rf:
            requested_restaurant = pickle.load(rf)
    else: # If it doesn't exist already, initialise.
        requested_restaurant = {}
        for r in restaurants.keys():
            requested_restaurant[r] = False

    # If there's progress from before, continue from there
    temp_collected_path = Path("../reviews_progress.pkl")
    if temp_collected_path.exists():
        with open(temp_collected_path, "rb") as rf:
            all_reviews = pickle.load(rf)
    else: # intialise container for reviews
        all_reviews = {}
    
    #### Loop through each naver searched restaurant
    for store_name, store_content in tqdm(restaurants.items()):
        if requested_restaurant.get(store_name, False):
            continue # If we've done it before, move on
        store_content_doesnt_exist = store_content is None # Flag variable for whether content exists for the restaurant
        if store_content_doesnt_exist or len(store_content) == 0:
            # If restaurant content doesn't exist, mark it done, and continue
            requested_restaurant[store_name] = True
            continue
        # From here, restaurant content DOES exist and we haven't requested it before
        # There could be multiple restaurants, even after filtering
        try:
            for one_store in store_content:
                    # get necessary information, for the API query + storing information
                    store_id = one_store["id"]
                    place_review_count = one_store["placeReviewCount"]
                    # If the restaurant doesn't have any naver map place reviews, SKIP!
                    if place_review_count == 0 or place_review_count == "0":
                        continue
                    cid_list = one_store["categoryPath"][0]
                    total_num_pages = ceil(place_review_count / 10)

                    # request for naver place reviews
                    all_review_items = []
                    # Must request for every page
                    for page_num in tqdm(range(1, total_num_pages+1)):
                        reviews = post_request_for_naver_place_reviews(store_id, cid_list, page_num)
                        # If we got something, store it
                        if reviews is not None:
                            all_review_items.extend(reviews[0]["data"]["visitorReviews"]["items"])
                        sleep(1) # to not overload the API and not make it seem suspicious
                    # Store all reviews for the store; each store identified by naver's id
                    all_reviews[store_id] = all_review_items
        except Exception as e:
            print("There was an error:", e)
            # If there's an error, save progress
            print("Saving progress...")
            with open(requested_restaurant_path, "wb") as wf:
                pickle.dump(requested_restaurant, wf)
            with open(temp_collected_path, "wb") as wf:
                pickle.dump(all_reviews, wf)
            there_was_an_error = True
            break
        else:
            # Mark the restaurant done
            requested_restaurant[store_name] = True
            there_was_an_error = False
    if there_was_an_error:
        print("Terminating early due to error...")
        return
    else:
        with open(reviews_path, "wb") as wf:
            pickle.dump(all_reviews, wf) # Save all reviews as pickled dictionary
        with open(reviews_json_path, "w") as wf:
            json.dump(all_reviews, wf, indent=4, ensure_ascii=False)
        # Delete temp files
        requested_restaurant_path.unlink()
        temp_collected_path.unlink()


if __name__ == "__main__":
    main()