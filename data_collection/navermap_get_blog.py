#%%
import re
import time
from math import ceil
from pathlib import Path
import pickle
import pandas as pd
import sqlite3
import duckdb
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from tqdm import tqdm, trange

######### Read data ##########################################
def get_restaurants_df(conn:duckdb.DuckDBPyConnection, table_name="restaurants") -> pd.DataFrame:
    """
    Retrieves the 'restaurants' table from a DuckDB database file as a Pandas DataFrame.

    Args:
        conn: Connection to database file

    Returns:
        A pandas DataFrame containing the 'restaurants' table data.
    """
    df = conn.table(table_name).df()
    return df
####### Collect blog review URLs #############################
def request_naver_blog_reviews(business_id:str, 
                               page=0, 
                               display=20):
    """
    Scrapes blog reviews from the Naver Place API.

    Args:
        business_id (str): The business ID.
        page (int): The page number of reviews to fetch (default: 0).
        display (int): The number of reviews per page (default: 20).

    Returns:
        dict: A dictionary containing the JSON response from the API, or None if an error occurred.
    """
    url = "https://pcmap-api.place.naver.com/graphql"
    headers = {
        'authority': 'pcmap-api.place.naver.com',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'ko',
        'content-type': 'application/json',
        'cookie': 'NNB=NOQ5IPEEUZ6GO; _fbp=fb.1.1744598109520.283917362332682121; ASID=b76d75f40000019632869f8b00000023; nstore_session=xgVCpz13V/+fJLNAc1DreETT; _ga=GA1.1.1385620538.1736215668; _ga_EFBDNNF91G=GS1.1.1744608711.1.0.1744608712.0.0.0; NAC=4QvVBkAlIfW0B; nid_inf=1949580813; NID_AUT=kl5afNNCajodpaWvKT4D5zEtB/uChF7Wcq3ih3RamPJ/RC7Z2y9iXzzJfFfWARjl; nstore_pagesession=ju2uadqqWd3Hldsntp8-150128; NACT=1; SRT30=1748332787; SRT5=1748332787; NID_SES=AAABqeYbvqsyMuk279OQPCT8CT4E6juGkzJCNBd7PqK3QyHXZh+HPFBBCuy20kEnEtYOnqrfApTHVhYFDm4U6MaHQ8ufiKYtEyFIsD7WJlVApvC9vVy4948F+z4VpMuwCWJSUjbij2L5FJQJxCSGguf+M0XNp6oVZn4ulNxk1yFPY6cTRKTShNKZpvbxtyjWm2UJRgFqk2mY9RlRtcLDY94PDk+1unorTYMABCRo6J6G+tZqyQNN8b0RigMN4UdSZo1EcB6PCERmqHrAbg+6tZcxAH/lyzdINw9neTQqAbVCKllVB7ylAdm0W8GArxJ4IsG5qmK2cuHRYWSDGRb09qNM8wi+59n16BoK+i5m8ogJsysodTUMlMvEGNWr34/tcOyUyaPIkgp8Zz86TyS+ubCStS6IGU7eWROQ3c010FfobxtPPscp1RpRTCkSadJeh9jH0lOWMvmM7VM1VVAXHbReC4KfnlRsF3nGhvdJA5ZmVDMBfG/9KOWSaKBPKVTg1S+g2AaC83DTxo6aa+hU2SxHCM0cVBd/N9DikGn+Tdj+3NAEIrSLjgIeyyDwT0CcHGkG5w==; PLACE_LANGUAGE=ko; BUC=50k4Qoncgb6-lHRtkkaelvcqGiYGsOTz63uvukh_2KY=',
        'origin': 'https://pcmap.place.naver.com',
        'priority': 'u=1, i',
        'referer': f'https://pcmap.place.naver.com/restaurant/{business_id}/review/',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'x-ncaptcha-violation': 'true',
        'x-wtm-graphql': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjE0MDEzMDEwODgiLCJ0eXBlIjoicmVzdGF1cmFudCIsInNvdXJjZSI6InBsYWNlIn0.YG-f9U-9o775-s594-5Y9V-w4-rX-6Y7n-rX-6Y7n-rX-6Y7n-rX-6Y7n', # This might change, but we'll use the provided one for now
        'x-gql-businessids': f'{business_id}',
        'x-gql-query-names': 'fsasReviews'
    }

    payload = [{
        "operationName": "getFsasReviews",
        "variables": {
            "input": {
                "businessId": business_id,
                "businessType": "restaurant",
                "page": page,
                "display": display,
                "deviceType": "mobile",
                "query": None,
                "excludeGdids": []
            }
        },
        "query": "query getFsasReviews($input: FsasReviewsInput) {\n  fsasReviews(input: $input) {\n    ...FsasReviews\n    __typename\n  }\n}\n\nfragment FsasReviews on FsasReviewsResult {\n  total\n  maxItemCount\n  items {\n    name\n    type\n    typeName\n    url\n    home\n    id\n    title\n    rank\n    contents\n    bySmartEditor3\n    hasNaverReservation\n    thumbnailUrl\n    thumbnailUrlList\n    thumbnailCount\n    date\n    isOfficial\n    isRepresentative\n    profileImageUrl\n    isVideoThumbnail\n    reviewId\n    authorName\n    createdString\n    bypassToken\n    __typename\n  }\n  __typename\n}"
    }]

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return None
    
def process_response(response:list) -> list:
    def is_blog(item):
        """Helper: Determines whether type is 'blog'"""
        return item.get("type") == "blog"
    if isinstance(response, list):
        all_items:list = []
        for rep in response:
            items_per_rep = rep.get("data", {}).get("fsasReviews", {}).get("items", [])
            if isinstance(items_per_rep, list):
                filtered = filter(is_blog, items_per_rep)
                all_items.extend(filtered)
            else:
                raise TypeError("Something went wrong when parsing this reponse:", response)
        return all_items
    else:
        raise TypeError("Something went wrong when parsing this reponse:", response)
    
def extract_url_from_item(item:dict) -> Any:
    if not isinstance(item, dict):
        return None
    return item.get("url")

def get_naver_blog_reviews_url(restaurants_table) -> dict:
    all_blog_reviews_url = {}
    num_of_all_r:int = len(restaurants_table)
    for i, restaurant in restaurants_table.iterrows():
        business_id = restaurant["naver_store_id"]
        store_name, nv_store_name = restaurant["store_name"], restaurant["naver_store_name"]
        blog_review_count = restaurant["naver_blog_review_count"]
        print(f"{i}/{num_of_all_r}: Collecting {blog_review_count} blog review URLs for {store_name}/{nv_store_name}...")
        if blog_review_count <= 0:
            continue
        total_num_pages = ceil(blog_review_count / 20)
        blog_review_urls:list = []
        for page_num in trange(total_num_pages):
            response = request_naver_blog_reviews(business_id, page=page_num, display=20) # 20 reviews per call
            if response:
                review_item_list = process_response(response)
                review_blog_url_list = map(extract_url_from_item, review_item_list)
                blog_review_urls.extend(review_blog_url_list)
            time.sleep(1)
        all_blog_reviews_url[business_id] = blog_review_urls
    return all_blog_reviews_url

####### Cached selenium html scraping #####################
def initialize_cache_db(cache_path: Path|str):
    """Initializes the SQLite database and creates the cache table."""
    with sqlite3.connect(cache_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cached_html (
                url TEXT PRIMARY KEY,
                html_content TEXT,
                timestamp REAL DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

def initialize_selenium_driver(headless: bool=False):
    """Initializes and returns a Selenium Chrome WebDriver."""
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu") # Required for headless on Windows
        options.add_argument("--no-sandbox") # Bypass OS security model
        options.add_argument("--disable-dev-shm-usage") # Overcome limited resource problems
    options.add_argument("--window-size=1920,1080") # Set a consistent window size

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    return driver

def get_html_cached(driver: webdriver.Chrome, url: str, cache_path: str|Path) -> bytes | None:
    """
    Attempts to retrieve HTML from the SQLite cache. If not found,
    it uses Selenium to fetch the HTML, scrolls, and caches it.
    """
    WAIT_TIME_AFTER_LOAD = 2
    WAIT_TIME_AFTER_SCROLL = 1
    # 1. Try to retrieve from cache
    with sqlite3.connect(cache_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT html_content FROM cached_html WHERE url = ?", (url,))
        cached_html = cursor.fetchone()

        if cached_html:
            return cached_html[0].encode('utf-8') # Return as bytes

    # 2. If not in cache, use Selenium to fetch
    # print(f"[{url}] - Not in cache. Fetching with Selenium...")
    try:
        driver.get(url)
        time.sleep(WAIT_TIME_AFTER_LOAD) # Wait for initial content

        # Scroll to the bottom to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(WAIT_TIME_AFTER_SCROLL) # Wait for lazy-loaded content

        html_content = driver.page_source.encode('utf-8')

        # 3. Store in cache
        cursor.execute("INSERT OR REPLACE INTO cached_html (url, html_content) VALUES (?, ?)",
                       (url, html_content.decode('utf-8'))) # Store as string
        conn.commit()
        # print(f"[{url}] - Fetched and cached successfully.")
        return html_content

    except Exception as e:
        print(f"[{url}] - Error fetching with Selenium: {e}")
        return None

###### Processing the html into data ##################################################################    
def extract_blog_info(soup:BeautifulSoup) -> Tuple[Dict,Any]:
    info_container = {}
    editorversion = None
    try:
        post_property = soup.find('div', id='_post_property')
        if isinstance(post_property, Tag):
            editorversion = post_property.get("editorversion")
            info_container["editorversion"] = editorversion
            info_container["blogname"] = post_property.get("blogname")
            info_container["attachvideoinfo"] = post_property.get("attachvideoinfo")
            commentcount = post_property.get("commentcount")
            if isinstance(commentcount, str):
                if commentcount.isdigit():
                    info_container["commentcount"] = int(commentcount) # type: ignore
            else:
                info_container["commentcount"] = None
    except Exception:
        info_container["editorversion"] = None
        info_container["blogname"] = None
        info_container["attachvideoinfo"] = None
        info_container["commentcount"] = None
    try:
        floating_property = soup.find(id="_floating_menu_property")
        if isinstance(floating_property, Tag):
            info_container["post_title"] = floating_property.get("posttitle")
    except Exception:
        info_container["post_title"] = None
    return info_container, editorversion
    
def we_can_handle_editorversion(editorversion:str) -> bool:
    if editorversion in ["1", "2", "3", "4"]:
        return True
    return False
    
def classify_editor_version(driver:webdriver.Chrome, db_path: Path|str, blog_urls:Dict[str, List[str]]) -> Dict[str, List[str]]:
    """To be used in scraping preparation"""
    editorversion_to_url:Dict[str, List[str]] = defaultdict(list)
    for _, url_list in tqdm(blog_urls.items()):
        for one_url in tqdm(url_list):
            blog_raw = get_html_cached(driver, one_url, db_path)
            if blog_raw:
                blog_soup = BeautifulSoup(blog_raw, "html.parser")
            else:
                continue
            _, editor_version = extract_blog_info(blog_soup)
            if isinstance(editor_version, str):
                editorversion_to_url[editor_version].append(one_url)
            else:
                print("Weird value in editor version:", editor_version)

    return editorversion_to_url

def get_post_date(editorversion:str, soup:BeautifulSoup) -> pd.Timestamp|None:
    """Get date information from blog post"""
    method = {
        "1": lambda soup: pd.Timestamp(soup.find(class_="se_date").text.strip()),
        "2": lambda soup: pd.Timestamp(soup.find(class_="se_date").text.strip()),
        "3": lambda soup: pd.Timestamp(soup.find(class_="blog_date").text.strip()),
        "4": lambda soup: pd.Timestamp(soup.find(class_="blog_date").text.strip())

    }
    try:
        post_date = method[editorversion](soup)
    except Exception:
        return None
    return post_date
def get_post_author(editorversion:str, soup:BeautifulSoup) -> str|None:
    method = {
        "1": lambda soup: soup.find(class_="se_author").text.strip(),
        "2": lambda soup: soup.find(class_="se_author").text.strip(),
        "3": lambda soup: soup.find(class_="blog_author").text.strip(),
        "4": lambda soup: soup.find(class_="blog_author").text.strip(),
    }
    try:
        post_author = method[editorversion](soup)
    except Exception:
        return None
    return post_author

def get_text(editorversion:str, soup:BeautifulSoup) -> str|None:
    """Get text from blog post"""
    # Helper function for getting rid of unnecessary whitespace/empty characters
    def process_text(text_part):
        text_0 = text_part.strip()
        text_1 = re.sub(r"\u200b", "", text_0)
        text_2 = re.sub(r"\xa0", "", text_1)
        result = text_2
        return result
    text_align_pattern = re.compile("text-align")
    
    method = {
        "1": lambda soup: "\n".join(map(lambda pp: process_text(pp.text), soup.find(id="viewTypeSelector").find_all("p"))),
        "2": lambda soup: "\n".join(map(lambda pp: process_text(pp.text), soup.find(id="viewTypeSelector").find_all(style=text_align_pattern))),
        "3": lambda soup: "\n".join(map(lambda pp: process_text(pp.text), soup.find(id="viewTypeSelector").find(class_="__se_component_area").find_all(class_="se_paragraph"))),
        "4": lambda soup: "\n".join(map(lambda pp: process_text(pp.text), soup.find(class_="se-main-container").find_all(class_="se-module-text")))
    }
    try:
        text = method[editorversion](soup)
    except Exception:
        return None
    return text

def search_for_img_url(img_item) -> str|None:
    """helper function for dealing with image urls"""
    large_src_pattern = re.compile("'(https:.*)'")
    if img_item.get("largesrc") is not None and len(img_item.get("largesrc")) > 0:
        img_src = img_item.get("largesrc")
        large_src_search_result = large_src_pattern.search(img_src)
        if large_src_search_result is not None:
            src_url = large_src_search_result.group(1)
        else:
            src_url = None
    elif img_item.get("data-lazy-src") is not None and len(img_item.get("data-lazy-src")) > 0:
        src_url = img_item.get("data-lazy-src")
    else:
        src_url = img_item.attrs.get("src")
    return src_url

def get_image_url_list(editorversion:str, soup:BeautifulSoup) -> List[str|None] | None:
    """Returns list of image URLs"""
    def extract_1(soup):
        all_post_images = soup.find(id="viewTypeSelector").find_all("img", class_="fx _postImage")
        img_url_list = list(map(search_for_img_url, all_post_images))
        if not img_url_list:
            return None
        return img_url_list
    def extract_2(soup):
        all_post_images = soup.find(id="viewTypeSelector").find_all("span", class_="_img fx")
        if all_post_images:
            all_post_img = map(lambda tag: tag.find("img"), all_post_images)
        else:
            all_post_img = []
        img_url_list = list(map(search_for_img_url, all_post_img))
        if not img_url_list:
            return None
        return img_url_list
    def extract_3(soup):
        all_post_images = soup.find_all("img", class_="se_mediaImage")
        if all_post_images:
            return list(map(search_for_img_url, all_post_images))
        else:
            return None
    def extract_4(soup):
        all_post_images = soup.find_all(class_="se-module-image")
        if all_post_images:
            all_post_img = map(lambda tag: tag.find("img"), all_post_images)
        else:
            all_post_img = []
        img_url_list = list(map(search_for_img_url, all_post_img))
        if not img_url_list:
            return None
        return img_url_list
    
    method = {
        "1": extract_1,
        "2": extract_2,
        "3": extract_3,
        "4": extract_4,

    }
    try:
        image_url_list = method[editorversion](soup)
    except Exception:
        return None
    return image_url_list

def get_sticker_url_list(editorversion:str, soup:BeautifulSoup) -> List[str|None] | None:
    """Returns list of sticker image URLs"""
    def extract_1(soup):
        return None
    def extract_2(soup):
        all_sticker_tags = soup.find(id="viewTypeSelector").find_all("img", class_="_sticker_img")
        if all_sticker_tags:
            result = list(map(search_for_img_url, all_sticker_tags))
            return result
        else:
            return None
    def extract_3(soup):
        all_stickers = soup.find_all(class_="se_sticker")
        all_sticker_imgs = list(map(lambda x: x.find("img"), all_stickers))
        if all_sticker_imgs:
            result = list(map(search_for_img_url, all_sticker_imgs))
            return result
        else:
            return None
    def extract_4(soup):
        all_stickers = soup.find_all(class_="se-sticker")
        all_sticker_imgs = list(map(lambda x: x.find("img"), all_stickers))
        if all_sticker_imgs:
            result = list(map(search_for_img_url, all_sticker_imgs))
            return result
        else:
            return None

    method = {
        "1":extract_1,
        "2": extract_2,
        "3": extract_3,
        "4": extract_4,

    }
    try:
        sticker_url_list = method[editorversion](soup)
    except Exception:
        return None
    return sticker_url_list

def get_vidthumb_url_list(editorversion:str, blog_info: dict, soup:BeautifulSoup) -> List[str|None] | None:
    """Returns list of video thumbnail image URLs"""
    video_attach_info = blog_info.get('attachvideoinfo')
    if video_attach_info:
        video_attached =  bool(video_attach_info) and len(video_attach_info) > 0
    else:
        video_attached = False
    if not video_attached:
        return None
    
    def extract_1(soup):
        return None
    def extract_2(soup):
        vid_thumb_pattern = re.compile("""url\("(.*)"\)""")

        all_thumbnails = soup.find_all(class_="pzp-poster")
        if all_thumbnails:
            url_areas = map(lambda x: x.find("style").text, all_thumbnails)
            vid_thumb_urls = list(map(lambda x: vid_thumb_pattern.search(x).group(1), url_areas))
            return vid_thumb_urls
        return None
    def extract_3(soup):
        return extract_2(soup)
    def extract_4(soup):
        return extract_2(soup)
    method = {
        "1":extract_1,
        "2": extract_2,
        "3": extract_3,
        "4": extract_4,
    }
    try:
        vidthumb_url_list = method[editorversion](soup)
    except Exception:
        return None
    return vidthumb_url_list
################ Full Process ############################################################################
def blog_url_is_collected(conn: duckdb.DuckDBPyConnection, 
                                url: str, 
                                table_name: str = "naverblog_reviews") -> bool:
    """
    Checks if a given blog post URL already exists in the specified DuckDB table.

    Args:
        conn (duckdb.DuckDBPyConnection): The active DuckDB connection.
        url (str): The URL of the blog post to check.
        table_name (str): The name of the table to query (default: "naverblog_reviews").

    Returns:
        bool: True if the URL is found in the table, False otherwise.
    """
    query = f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE post_url = ?);"
    result = conn.execute(query, [url]).fetchone()
    
    # fetchone() returns a tuple, and the EXISTS result is the first element
    if result and result[0] == 1:
        return True
    return False

def collect_blog_post_data(url, soup) -> Dict[str, str|Any]:
    """Collect data about one particular blog post URL"""
    row = {}
    row["post_url"] = url

    blog_info, editorversion = extract_blog_info(soup)

    if not we_can_handle_editorversion(editorversion) and editorversion is not None:
        print("New blog editor version! We can't handle this yet!")
        print(f"editorversion: {editorversion}")
    
    row.update(blog_info)
    row.pop("attachvideoinfo", None) # Don't need this

    row["post_date"] = get_post_date(editorversion, soup)
    row["author"] = get_post_author(editorversion, soup)
    row["text"] = get_text(editorversion, soup)
    row["img_url"] = get_image_url_list(editorversion, soup)
    row["sticker_url"] = get_sticker_url_list(editorversion, soup)
    row["vid_thumb_url"] = get_vidthumb_url_list(editorversion, blog_info, soup)
    
    return row

def collect_blog_reviews(driver:webdriver.Chrome, 
                         cache_path: Path|str,
                         conn: duckdb.DuckDBPyConnection,
                         blog_urls:Dict[str, List[str]],
                         table_name:str = "naverblog_reviews",):
    """Collect data about naver blog posts; Back up to DB
    Args:
        driver(webdriver.Chrome): selenium driver
        cache_path(Path|str): path to scraping cache database
        blog_urls(Dict[str, List[str]]): Key - store_id; Value - list of blog review URLs
    """    
    for store_id, url_list in tqdm(blog_urls.items()):
        store_container = []
        for one_url in tqdm(url_list):
            if blog_url_is_collected(conn, one_url):
                continue
            blog_raw = get_html_cached(driver, one_url, cache_path)
            if blog_raw:
                blog_soup = BeautifulSoup(blog_raw, "html.parser")
            else:
                continue
            row = collect_blog_post_data(one_url, blog_soup)
            if row:
                store_container.append(row)
        store_df = pd.DataFrame(store_container)
        store_df = store_df.drop_duplicates(["post_url"], ignore_index=True)
        update_blog_reviews_db(table_name= table_name, 
                               conn=conn,
                               df=store_df)
        print(f"Updated store id {store_id}")
    blog_reviews = get_blog_reviews_from_db(table_name, conn)
    return blog_reviews

def initialise_blog_reviews(conn:duckdb.DuckDBPyConnection,
                            table_name:str="naverblog_reviews"):
    q1 = f"DROP TABLE IF EXISTS {table_name};"
    conn.execute(q1)
    q2 = f"""CREATE OR REPLACE TABLE {table_name} (
            post_url VARCHAR PRIMARY KEY,
            editorversion VARCHAR,
            blogname VARCHAR,
            commentcount INTEGER,
            post_title VARCHAR,
            post_date TIMESTAMP,
            author VARCHAR,
            text VARCHAR,
            img_url VARCHAR[],
            sticker_url VARCHAR[],
            vid_thumb_url VARCHAR[]
            )"""
    conn.execute(q2)

def update_blog_reviews_db(table_name:str, 
                           conn:duckdb.DuckDBPyConnection,
                           df:pd.DataFrame|None=None):
    try:
        if df is None or df.empty:
            return
            # working_columns = ", ".join(df.columns)
        df_rel = conn.from_df(df)
        df_rel.insert_into(table_name)
    except Exception as e:
        raise e


def get_blog_reviews_from_db(table_name:str,
                             conn:duckdb.DuckDBPyConnection,) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name};"
    df = conn.sql(query).df()
    return df

def main(db_path = Path(__file__).parent.parent / "dataset/reviews_temp.db",
         blog_urls_path = Path(r"G:\My Drive\Data\naver_search_results\naverblog_urls.pkl"),
         blog_reviews_path = Path(r"G:\My Drive\Data\naver_search_results\naverblog_reviews.parquet.gzip"),
         db_initialisation = False):
    CACHE_NAME = "naverblog.sqlite"
    CWD = Path.cwd()
    CACHE_PATH = CWD / CACHE_NAME
    if not CACHE_PATH.exists():
        initialize_cache_db(CACHE_PATH)

    with duckdb.connect(db_path) as conn:
        if not blog_urls_path.exists():
            #### Get restaurant list
            print("Load restaurant list...")
            restaurants_table = get_restaurants_df(conn)
            
            #### Collect blog review urls from naver place
            print("Collecting blog review URLs from NAVER Place...")
            blog_url = get_naver_blog_reviews_url(restaurants_table)

            #### Save blog review urls
            print("Saving blog review URLs...")
            with open(blog_urls_path, "wb") as wf:
                pickle.dump(blog_url, wf)
        else:
            ############ Continue from blog_urls #############################
            print("Found NAVER blog review URLs. Loading...")
            with open(blog_urls_path, "rb") as rf:
                blog_urls = pickle.load(rf)

        driver = initialize_selenium_driver(headless=False)
        table_name = "naverblog_reviews"
        if db_initialisation:
            print("Initialise table in DB...")
            initialise_blog_reviews(conn, table_name)
        try:
            final_blog_reviews = collect_blog_reviews(driver=driver, 
                                                    cache_path=CACHE_PATH, 
                                                    conn=conn,
                                                    blog_urls=blog_urls,
                                                    table_name=table_name)
            final_blog_reviews.to_parquet(blog_reviews_path, compression="gzip")
        except Exception as e:
            driver.quit()
            raise e
        else:
            driver.quit()
#%%
if __name__ == "__main__":
    main(db_initialisation=False) 
    
