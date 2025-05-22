#%%
import requests
import json
import requests_cache
import pandas as pd
from pathlib import Path
import pickle
from time import sleep
from tqdm import tqdm
import re

CACHE_NAME = 'naver_map_cache'
OUTPUT_DIR = Path('G:/My Drive/Data/naver_search_results')
OUTPUT_DIR.mkdir(exist_ok=True)

# Setup the cache
session = requests_cache.CachedSession(CACHE_NAME, expire_after=3600)

def get_access_token(sgis_id:str, sgis_secret:str) -> str:
    """SGIS에서 accessToken 받기; 매 세션마다 받아야 함."""
    r = requests.get("https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json",
                     params= {
                         "consumer_key": sgis_id,
                         "consumer_secret": sgis_secret
                     })
    return r.json().get("result").get("accessToken")

# import sensitive information
with open("../haein_secrets.json", "r") as f:
    secrets = json.load(f)
ACCESS_TOKEN = get_access_token(secrets["sgis_id"], secrets["sgis_secret"])

from pyproj import CRS, Transformer

def convert_epsg5174_to_wgs84(x, y):
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
        return None
    
def sanitize_filename(text):
    """Removes or replaces characters that might cause issues in filenames."""
    return "".join(c if c.isalnum() else "_" for c in text)

def get_naver_search_data(url, query, coords, headers=secrets["naver_header"]):
    """Retrieves Naver search data, using caching."""
    params = {
        "query": query,
        "searchCoord": coords
    }
    try:
        response = session.get(url, params=params, headers=headers)
        response.raise_for_status()
        if response.json() is not None:
            return response.json()
        else:
            return {}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for '{query}': {e}")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON for '{query}'")
        return {}
    except Exception as e:
        print(f"Unknown error while getting results for {query}:,", e)
        return {}
    

def save_result_to_file(query, data):
    """Saves the API result to a JSON file."""
    filename = OUTPUT_DIR / f"{sanitize_filename(query)}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Saved result to: {filename}")
    except IOError as e:
        print(f"Error saving result to file: {e}")


def prepare_restaurant_list(mapogu_path =Path("../dataset/seoul_mapogu_general_restaurants.xlsx")):
    ## Prepare list of restaurants
    dtype = {'도로명우편번호': str}
    mapogu = pd.read_excel(mapogu_path, dtype=dtype)
    focus_region = "연남동"
    mapogu = mapogu.dropna(subset = ["소재지전체주소", "도로명전체주소"])
    region_df = mapogu[mapogu["도로명전체주소"].str.contains(focus_region)]
    region_df = region_df.dropna(how="all", axis=1)
    search_df = region_df[["사업장명", "좌표정보X(EPSG5174)", '좌표정보Y(EPSG5174)', "소재지전체주소", "도로명전체주소"]].copy()
    search_df = search_df.rename(columns = {"사업장명": "store_name",
                                            "좌표정보X(EPSG5174)": "X_before",
                                            '좌표정보Y(EPSG5174)': "Y_before",
                                            "소재지전체주소": "jibun_address",
                                            "도로명전체주소": "road_address"})
    search_df.drop_duplicates()
    return search_df
def sgis_converter(access_token, og_x, og_y, to_utmk=True):
    if to_utmk:
        current_coords = "4326"
        change_coords = "5179"
    else:
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
        print("Error fetching data")
        return None, None
    if response:
        new_x = response.json().get("result").get("posX")
        new_y = response.json().get("result").get("posY")
    else:
        return None, None

    return new_x, new_y


def get_dong_from_utmk(access_token, umtk_x, umtk_y):
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



def get_failed_rows(failed_rows_path = Path("failed_rows.pkl")):
    with open(failed_rows_path, "rb") as f:
        failed_rows = pickle.load(f)
    return failed_rows
def save_failed_rows(failed_rows, failed_rows_path = Path("failed_rows.pkl")):
    with open(failed_rows_path, "wb") as f:
        pickle.dump(failed_rows, f)

def extract_names(store_name):
    """
    괄호 속 사업장명 추출하기
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
    else: return False


def search_through_places(result_places, access_token):
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
                                                     "연남동")
        if is_in_region:
            filtered.append(place)
    return filtered

#%%
if __name__ == "__main__":
    ################ Request for search results ##############################################
    CACHE_NAME = 'naver_map_cache'
    OUTPUT_DIR = Path('G:/My Drive/Data/naver_search_results')
    OUTPUT_DIR.mkdir(exist_ok=True)
    # Setup the cache
    session = requests_cache.CachedSession(CACHE_NAME, expire_after=3600)

    # import sensitive information
    with open("../haein_secrets.json", "r") as f:
        secrets = json.load(f)
    ACCESS_TOKEN = get_access_token(secrets["sgis_id"], secrets["sgis_secret"])


    search_url = "https://map.naver.com/p/api/search/allSearch"
    failed_rows_path = Path("failed_rows.pkl")
    if failed_rows_path.exists():
        search_df = get_failed_rows()
    else:
        search_df = prepare_restaurant_list()
    failed_rows=[]
    all_search_results = {}
    for i, row in tqdm(search_df.iterrows()):
        store_name = row["store_name"]
        print(f"Working on {store_name}...")
        X_before, Y_before = row["X_before"], row["Y_before"]
        X_after, Y_after = convert_epsg5174_to_wgs84(X_before, Y_before)
        search_results = get_naver_search_data(search_url, store_name, f"{X_after};{Y_after}", secrets["naver_header"])
        sleep(1)
        # condition to check if we actually got place results from the search
        retrieved_a_place = bool(search_results.get("result", {}).get("place"))
        if retrieved_a_place:
            actually_retrieved_places = not (len(search_results.get("result", {}).get("place", {}).get("list", [])) == 0)
        else:
            actually_retrieved_places = False
        if actually_retrieved_places:
            print(f"Success at {store_name}")
            print("totalCount:", search_results.get("result", {}).get("place", {}).get("totalCount", 0))
            result_places = search_results.get("result", {}).get("place", {}).get("list", [])
            filtered_results = search_through_places(result_places, ACCESS_TOKEN)
            
            print("After filtering:", len(filtered_results))
            all_search_results[store_name] = filtered_results
        else:
            failed_rows.append(row)
            print(search_results)
            print(f"failed to get results for {store_name}")
    save_result_to_file("mapogu_yeonnamdong_naver", all_search_results)
    if failed_rows:
        save_failed_rows(pd.DataFrame(failed_rows))
    else:
        failed_rows_path.unlink(missing_ok=True)




# %%
############ Checks how many were collected
# with open(OUTPUT_DIR / "mapogu_yeonnamdong_naver.json", "r") as f:
#     result = json.load(f)
# count = 0
# for k, v in result.items():
#     if len(v) > 0:
#         count += 1
# print(count)
