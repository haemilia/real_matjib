#%%
import requests
import requests_cache
from datetime import timedelta
import pandas as pd
from pathlib import Path
import pickle
import json
from time import sleep
from tqdm import tqdm
import re
from typing import Tuple, Any

def get_access_token(sgis_id:str, sgis_secret:str) -> str:
    """SGIS에서 accessToken 받기; 매 세션마다 받아야 함."""
    r = requests.get("https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json",
                     params= {
                         "consumer_key": sgis_id,
                         "consumer_secret": sgis_secret
                     })
    return r.json().get("result").get("accessToken")

def convert_epsg5174_to_wgs84(x:float, y:float) -> Tuple[Any, Any]:
    """
    Converts coordinates from EPSG:5174 to WGS 84 (EPSG:4326).
    Not super accurate...

    Args:
        x (float): The Easting coordinate (x) in EPSG:5174.
        y (float): The Northing coordinate (y) in EPSG:5174.

    Returns:
        tuple: A tuple containing (longitude(x), latitude(y)) in WGS 84.
               Returns None if the transformation fails.
    """
    from pyproj import CRS, Transformer
    try:
        # Define the source coordinate system (EPSG:5174)
        crs_from = CRS.from_proj4("+proj=tmerc +lat_0=38 +lon_0=127.002890277778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +towgs84=-145.907,505.034,685.756,-1.162,2.347,1.592,6.342 +units=m +no_defs")

        # Define the target coordinate system (WGS 84 - EPSG:4326)
        crs_to = CRS.from_epsg(4326)

        # Create a transformer
        transformer = Transformer.from_crs(crs_from, crs_to)

        # Perform the transformation
        latitude, longitude = transformer.transform(x, y)

        return longitude, latitude
    except Exception as e:
        print(f"An error occurred during the transformation: {e}")
        return None, None
    
def sanitize_filename(text:str) -> str:
    """Removes or replaces characters that might cause issues in filenames."""
    return "".join(c if c.isalnum() else "_" for c in text)


def prepare_restaurant_list(focus_region:str, raw_restaurant_path:Path) -> pd.DataFrame:
    """
    Reads excel file of 일반음식점 into a dataframe. After filtering for `focus_region`, returns a pandas DataFrame with the following columns:
    - store_name
    - X_before
    - Y_before
    - jibun_address
    - road_address
    Args:
        focus_region (str): Name of a region. Filter the dataset to only restaurants inside that region.
        raw_restaurant_path (Path): Path to excel file containing list of 일반음식점 

    """
    ## Prepare list of 일반음식점점
    dtype = {'도로명우편번호': str}
    mapogu = pd.read_excel(raw_restaurant_path, dtype=dtype)
    mapogu = mapogu.dropna(subset = ["소재지전체주소", "도로명전체주소"])
    ## Filter restaurants inside focus_region
    region_df = mapogu[mapogu["도로명전체주소"].str.contains(focus_region)]
    ## Drop rows with all null values
    region_df = region_df.dropna(how="all", axis=1)
    ## Select columns of interest
    search_df = region_df[["사업장명", "좌표정보X(EPSG5174)", '좌표정보Y(EPSG5174)', "소재지전체주소", "도로명전체주소"]].copy()
    ## Rename columns
    search_df = search_df.rename(columns = {"사업장명": "store_name",
                                            "좌표정보X(EPSG5174)": "X_before",
                                            '좌표정보Y(EPSG5174)': "Y_before",
                                            "소재지전체주소": "jibun_address",
                                            "도로명전체주소": "road_address"})
    ## Drop duplicate rows
    search_df.drop_duplicates(inplace=True)
    return search_df


def sgis_converter(access_token:str, og_x:float|str, og_y:float|str, to_utmk=True) -> Tuple[Any|None, Any|None]:
    if to_utmk: # Default behaviour
        current_coords = "4326"
        change_coords = "5179"
    else: # Opposit behaviour
        current_coords = "5179"
        change_coords = "4326"
    try:
        response = requests.get("https://sgisapi.kostat.go.kr/OpenAPI3/transformation/transcoord.json",
                    params= {
                        "accessToken": access_token,
                        "src": current_coords,
                        "dst": change_coords,
                        "posX": str(og_x),
                        "posY": str(og_y)
                    })
        response.raise_for_status()
        

    except requests.exceptions.RequestException as e:
        print("Error fetching data:", e)
        return None, None
    if response:
        new_x = response.json().get("result").get("posX")
        new_y = response.json().get("result").get("posY")
    else:
        return None, None

    return new_x, new_y


def get_dong_from_utmk(access_token:str, umtk_x:float|str, umtk_y:float|str) -> Any|None:
    r = requests.get("https://sgisapi.kostat.go.kr/OpenAPI3/addr/rgeocode.json",
                params={
                    "accessToken": access_token,
                    "x_coor": umtk_x,
                    "y_coor": umtk_y,
                    "addr_type": 20
                })
    if r.json().get("result"):
        dong_info = r.json().get("result")[0].get("emdong_nm")
    else:
        dong_info = None
    return dong_info


def extract_names(store_name):
    """
    괄호 속 사업장명 추출하기
    Not Used?
    """
    match = re.search(r"^(.*?)\s*?\((.*?)\)$", store_name)
    if match:
        name_before = match.group(1).strip()
        name_inside = match.group(2).strip()
        return name_before, name_inside
    else:
        return store_name, None
    
def naver_coords_is_in_region(access_token, naver_X, naver_Y, region_name="연남동") -> bool:
    umtk_x, umtk_y = sgis_converter(access_token, naver_X, naver_Y)
    if umtk_x and umtk_y:
        dong_info = get_dong_from_utmk(access_token, umtk_x, umtk_y)
    else:
        print("sgis_converter didn't work")
        dong_info = None
    if dong_info and (dong_info == region_name or region_name in dong_info):
        return True
    else: 
        return False


def search_through_places(result_places:list, access_token:str, region_of_focus:str) -> list:
    if not isinstance(result_places, list):
        return []
    filtered = []
    for place in tqdm(result_places):
        naver_x = place.get("x")
        naver_y = place.get("y")
        if naver_x and naver_y:
            is_in_region = naver_coords_is_in_region(access_token,
                                                     naver_x,
                                                     naver_y, 
                                                     region_of_focus)
        else:
            is_in_region = False
        if is_in_region:
            filtered.append(place)
    return filtered

    
def send_request_for_restaurant_search(session:requests_cache.CachedSession, restaurant_name:str, coordinate_string:str) -> dict:
    search_url = "https://map.naver.com/p/api/search/allSearch"

    headers = {
        "authority": "map.naver.com",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4",
        "cache-control": "no-cache",
        "cookie": "NACT=1; NNB=NOQ5IPEEUZ6GO; _fbp=fb.1.1744598109520.283917362332682121; ASID=b76d75f40000019632869f8b00000023; nstore_session=xgVCpz13V/+fJLNAc1DreETT; _ga=GA1.1.1385620538.1736215668; _ga_EFBDNNF91G=GS1.1.1744608711.1.0.1744608712.0.0.0; NAC=4QvVBkAlIfW0B; nid_inf=1949580813; NID_AUT=kl5afNNCajodpaWvKT4D5zEtB/uChF7Wcq3ih3RamPJ/RC7Z2y9iXzzJfFfWARjl; nstore_pagesession=juKb2wqWLGk79lsM3pV-387156; NACT=1; SRT30=1747633771; NID_SES=AAABqZijlXW6Z4g/3pl+J/3lJPixxkPzWb+E1ZdkHIRui7P8O8Y0i6jmOOryG5iObnq2uW0Ojrm0ex2MEcKKaDmCkOZ03wZObG3CLUP/mYA9nD0bxSjTO4GsYvg/g6NNHe5Qi664eAh9lX4PCsGtVSwig/cc/LD4zBTnjzc8qe6jDkd6wcw0yBdySgPewK6DNNMbyr+64AV94g00X+sGnQeeVdnvTFTlhfIya4NakT5WjdNNqucnaPgaNqeOJvNsaIvlHcjwMyIFuVQLi0ac+q85HpFbIg7B0fvdgKfIWRdsWl1Pwbz+LcU31b8Viguk3tegwM5RgSdWQuO8gDcE7sEI6rNk8+84e9SmBmQkYSFb+zdL5hTjvjKlXSEFplD2hFPrjYsKGKQWfg7u90e+aEAGAzgkrsf3VryeAnUWz84hVgRF8wZQsp3qo5Co4IpLt5ljSSs5hHLWX0B61rp9L5moTxdWvalG7bqbhZmUjq3mqei7aNzW6NMCABTsnDxaDup/VykClASRKB9KtsY4NtmTw0m038q+FcLBfCiRd1fRVu/82Vtn3xj4GMOcsodBAYgUjg==; SRT5=1747639302; BUC=lhjo-xhF3sOoH3p82tImPwDzNu-AEi5ms-VZYoFwC_k=",
        "referer": "https://map.naver.com/p/search/%EC%97%B0%EB%82%A8%EB%8F%99%20%EB%A7%9B%EC%A7%91?c=15.00,0,0,0,dh",
        "sec-ch-ua": "\"Chromium\";v=\"136\", \"Google Chrome\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    params = {
        "query": restaurant_name,
        "searchCoord": coordinate_string,
    }
    response = None

    try:
        response = session.get(search_url, params=params, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        if response.json() is not None:
            return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for '{restaurant_name}': {e}")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON for '{restaurant_name}'. Try with UTF-8")
        try:
            if response is not None:
                return json.loads(response.content.decode('utf-8'))
            else:
                return {}
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON with UTF-8 for '{restaurant_name}': {e}")
            return {}
    except Exception as e:
        print(f"Unknown error while getting results for {restaurant_name}:,", e)
        return {}
    else:
        return {}
    
def get_failed_rows(failed_rows_path:Path) -> Tuple[pd.DataFrame, bool]:
    """If past failed attempts file is found, return the failed rows, else return empty list. Also return `requery_allowed`, a flag variable denoting whether to proceed with re-querying failed rows or not.
    Args:
        failed_rows_path (Path): path to file that contains past failed attempts.
    """
    if failed_rows_path.exists():
        failed_rows:pd.DataFrame = pd.read_pickle(failed_rows_path)
        requery_allowed = True
        return failed_rows, requery_allowed
    else:
        requery_allowed = False
        return pd.DataFrame([]), requery_allowed

def check_for_valid_results(results:dict)-> int:
    valid_count = 0
    for k, v in results.items():
        if len(v)> 0:
            valid_count += 1
    return valid_count

def main(DATASET_DIR=Path("../dataset"), 
         BACKUP_STORAGE_DIR=Path('G:/My Drive/Data/naver_search_results'), 
         CACHE_NAME='naver_map_cache', 
         REGION_OF_FOCUS="연남동", 
         SECRETS_FILE=Path("../haein_secrets.json"),
         restaurants_raw_path=Path("../dataset/seoul_mapogu_general_restaurants.xlsx"),
         restaurants_path=Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_.pkl"),
         restaurants_json_path=Path("G:/My Drive/Data/naver_search_results/mapogu_yeonnamdong_naver_.json")):
    
    #### Prepare environment
    DATASET_DIR.mkdir(exist_ok=True)
    BACKUP_STORAGE_DIR.mkdir(exist_ok=True)

    # Setup the cache
    session = requests_cache.CachedSession(CACHE_NAME, expire_after=timedelta(days=30))

    #### import sensitive information
    with open(SECRETS_FILE, "r") as f:
        secrets = json.load(f)
    ACCESS_TOKEN = get_access_token(secrets["sgis_id"], secrets["sgis_secret"])

    # If there were any failed attempts, then we re-query them
    failed_rows_path = BACKUP_STORAGE_DIR / "failed_rows.pkl"
    failed_rows, requery_failed_rows = get_failed_rows(failed_rows_path)
    # Otherwise, we query our list of restaurants

    # restaurants_raw_path = DATASET_DIR / "seoul_mapogu_general_restaurants.xlsx"
    # restaurants_path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver_.pkl"

    if requery_failed_rows:
        print("Past failed attempts found! Preparing for re-query...")
        search_df = failed_rows # re-query
        failed_rows = [] # empty this to contain failed rows of this new attempt
        with open(restaurants_path, "rb") as rf:
            all_restaurants = pickle.load(rf) # load previous search results, so we can continue
    else:
        print(f"No past failed attempts found. Preparing naver map restaurant search in area '{REGION_OF_FOCUS}'")
        search_df = prepare_restaurant_list(focus_region=REGION_OF_FOCUS,
                                                         raw_restaurant_path=restaurants_raw_path)
        all_restaurants = {} # Container for restaurant search results 
        failed_rows = [] # Container for failed rows 
    
    #### Loop through all restaurants in raw restaurant list
    for i, row in tqdm(search_df.iterrows()):
        # Prepare for restaurant search
        store_name = row["store_name"]
        print(f"Working on {store_name}...")
        X_before, Y_before = row["X_before"], row["Y_before"]
        X_after, Y_after = convert_epsg5174_to_wgs84(X_before, Y_before) # Convert from EPSG 5174 to WGS 84
        coord_str = f"{X_after};{Y_after}"

        ################ REQUEST FOR SEARCH RESULTS ##############################################
        search_results = send_request_for_restaurant_search(session, store_name, coord_str)
        sleep(1) # To avoid overloading the API

        ######### FILTER THROUGH SEARCH RESULTS FOR OUR RESTAURANT ############################################
        # condition to check if we actually got place results from the search
        retrieved_a_place = bool(search_results.get("result", {}).get("place"))
        if retrieved_a_place:
            actually_retrieved_places = not (len(search_results.get("result", {}).get("place", {}).get("list", [])) == 0)
        else:
            actually_retrieved_places = False

        # Filter process
        if actually_retrieved_places:
            print(f"Success at {store_name}")
            print("totalCount:", search_results.get("result", {}).get("place", {}).get("totalCount", 0))

            result_places = search_results.get("result", {}).get("place", {}).get("list", []) # Only get the places
            filtered_results = search_through_places(result_places=result_places, 
                                                     access_token=ACCESS_TOKEN, 
                                                     region_of_focus=REGION_OF_FOCUS) # Check location of each of the places results
            
            print("After filtering:", len(filtered_results))

            all_restaurants[store_name] = filtered_results
        else:
            failed_rows.append(row)
            print(search_results)
            print(f"failed to get results for {store_name}")
    
    # Calculate how many queries yielded valid results
    queries_with_valid_results = check_for_valid_results(all_restaurants)
    print(f"A total of {len(all_restaurants)} restaurants queried. {queries_with_valid_results} valid restaurant queries.")

    # Save results dictionary to file
    print(f"Saving all restaurant search results at {restaurants_path}...")
    with open(restaurants_path, "wb") as wf:
        pickle.dump(all_restaurants, wf)
    with open(restaurants_json_path, "w") as wf:
        json.dump(all_restaurants, wf)
    
    # If there were failed rows save that too
    if len(failed_rows) > 0:
        failed_rows = pd.DataFrame(failed_rows)
        print(f"There were {len(failed_rows)} failed rows. Saving...")
        failed_rows.to_pickle(failed_rows_path)
    else:
        # If there were no failed rows, we don't need the file any more.
        print("There were no failed rows! Deleting failed rows file...")
        failed_rows_path.unlink(missing_ok=True)

if __name__ == "__main__":
    main()


    # ################ Request for search results ##############################################
    # DATASET_DIR = Path("../dataset")
    # DATASET_DIR.mkdir(exist_ok=True)
    # BACKUP_STORAGE_DIR = Path('G:/My Drive/Data/naver_search_results')
    # BACKUP_STORAGE_DIR.mkdir(exist_ok=True)

    # CACHE_NAME = 'naver_map_cache'

    # REGION_OF_FOCUS = "연남동"
    
    # # Setup the cache
    # session = requests_cache.CachedSession(CACHE_NAME, expire_after=3600)

    # # import sensitive information
    # with open("../haein_secrets.json", "r") as f:
    #     secrets:dict = json.load(f)
    # ACCESS_TOKEN = get_access_token(secrets["sgis_id"], secrets["sgis_secret"])

    # # If there were any failed attempts, then we re-query them
    # failed_rows_path = BACKUP_STORAGE_DIR / "failed_rows.pkl"
    # failed_rows, requery_failed_rows = get_failed_rows(failed_rows_path)
    # # Otherwise, we query our list of restaurants
    # restaurants_raw_path = DATASET_DIR / "seoul_mapogu_general_restaurants.xlsx"
    # restaurants_naver_search_path = BACKUP_STORAGE_DIR / "mapogu_yeonnamdong_naver_.pkl"
    # if requery_failed_rows:
    #     print("Past failed attempts found! Preparing for re-query...")
    #     search_df:pd.DataFrame = failed_rows # re-query
    #     failed_rows = [] # empty this to contain failed rows of this new attempt
    #     with open(restaurants_naver_search_path, "rb") as rf:
    #         all_restaurants = pickle.load(rf) # load previous search results, so we can continue
    # else:
    #     print(f"No past failed attempts found. Preparing naver map restaurant search in area '{REGION_OF_FOCUS}'")
    #     search_df:pd.DataFrame = prepare_restaurant_list(REGION_OF_FOCUS,restaurants_raw_path)
    #     all_restaurants = {} # Container for restaurant search results 
    
    # for i, row in tqdm(search_df.iterrows()):
    #     # Prepare for restaurant search
    #     store_name:str = row["store_name"]
    #     print(f"Working on {store_name}...")
    #     X_before, Y_before = row["X_before"], row["Y_before"]
    #     X_after, Y_after = convert_epsg5174_to_wgs84(X_before, Y_before) # Convert from EPSG 5174 to WGS 84
    #     coord_str:str = f"{X_after};{Y_after}"

    #     ################ REQUEST FOR SEARCH RESULTS ##############################################
    #     search_results:dict = send_request_for_restaurant_search(session, store_name, coord_str)
    #     sleep(1) # To avoid overloading the API

    #     ######### FILTER THROUGH SEARCH RESULTS FOR OUR RESTAURANT ############################################
    #     # condition to check if we actually got place results from the search
    #     retrieved_a_place = bool(search_results.get("result", {}).get("place"))
    #     if retrieved_a_place:
    #         actually_retrieved_places = not (len(search_results.get("result", {}).get("place", {}).get("list", [])) == 0)
    #     else:
    #         actually_retrieved_places = False

    #     # Filter process
    #     if actually_retrieved_places:
    #         print(f"Success at {store_name}")
    #         print("totalCount:", search_results.get("result", {}).get("place", {}).get("totalCount", 0))

    #         result_places:list = search_results.get("result", {}).get("place", {}).get("list", []) # Only get the places
    #         filtered_results:list = search_through_places(result_places=result_places, 
    #                                                  access_token=ACCESS_TOKEN, 
    #                                                  region_of_focus=REGION_OF_FOCUS) # Check location of each of the places results
            
    #         print("After filtering:", len(filtered_results))

    #         all_restaurants[store_name] = filtered_results
    #     else:
    #         failed_rows.append(row)
    #         print(search_results)
    #         print(f"failed to get results for {store_name}")
    
    # # Calculate how many queries yielded valid results
    # queries_with_valid_results = check_for_valid_results(all_restaurants)
    # print(f"A total of {len(all_restaurants)} restaurants queried. {queries_with_valid_results} valid restaurant queries.")

    # # Save results dictionary to file
    # print(f"Saving all restaurant search results at {restaurants_naver_search_path}...")
    # with open(restaurants_naver_search_path, "wb") as wf:
    #     pickle.dump(all_restaurants, wf)
    
    # # If there were failed rows save that too
    # if len(failed_rows) > 0:
    #     failed_rows = pd.DataFrame(failed_rows)
    #     print(f"There were {len(failed_rows)} failed rows. Saving...")
    #     failed_rows.to_pickle(failed_rows_path)
    # else:
    #     # If there were no failed rows, we don't need the file any more.
    #     print("There were no failed rows! Deleting failed rows file...")
    #     failed_rows_path.unlink(missing_ok=True)
