#%%
from pathlib import Path
import pickle
import pandas as pd
import sqlite3
import duckdb
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from tqdm import tqdm
import re
import time

####### Cached selenium html scraping #####################
def initialize_db(db_path: Path|str):
    """Initializes the SQLite database and creates the cache table."""
    with sqlite3.connect(db_path) as conn:
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
    else:
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

def search_for_img_url(img_item):
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
                         cache_path: Path|str, blog_urls:Dict[str, List[str]]):
    """Collect data about naver blog posts; Back up to DB
    Args:
        driver(webdriver.Chrome): selenium driver
        cache_path(Path|str): path to scraping cache database
        blog_urls(Dict[str, List[str]]): Key - store_id; Value - list of blog review URLs
    """
    table_name = "naverblog_reviews"
    print("Create table in DB...")
    update_blog_reviews_db(table_name, create=True)
    for store_id, url_list in tqdm(blog_urls.items()):
        store_container = []
        for one_url in tqdm(url_list):
            blog_raw = get_html_cached(driver, one_url, cache_path)
            if blog_raw:
                blog_soup = BeautifulSoup(blog_raw, "html.parser")
            else:
                continue
            row = collect_blog_post_data(one_url, blog_soup)
            if row:
                store_container.append(row)
        store_df = pd.DataFrame(store_container)
        update_blog_reviews_db(table_name= table_name, df=store_df)
        print(f"Updated store id {store_id}")
    blog_reviews = get_blog_reviews_from_db(table_name)
    return blog_reviews

def update_blog_reviews_db(table_name:str, 
                           df:pd.DataFrame|None=None, 
                           db_path=Path("../dataset/reviews.db"), 
                           create=False):
    try:
        with duckdb.connect(db_path) as conn:
            if create:
                conn.execute("DROP TABLE IF EXISTS naverblog_reviews;")
                conn.execute("DROP SEQUENCE IF EXISTS post_id_seq;")
                conn.execute("CREATE SEQUENCE post_id_seq START 1;")
                conn.execute("""CREATE OR REPLACE TABLE naverblog_reviews (
                        post_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('post_id_seq'),
                        post_url VARCHAR,
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
                      )""")
            elif df is not None:
                query = f"INSERT INTO {table_name} SELECT * FROM df"
                conn.execute(query)
    except Exception as e:
        raise e

def get_blog_reviews_from_db(table_name:str,
                             db_path=Path("../dataset/reviews.db"),) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name};"
    with duckdb.connect(db_path) as conn:
        df = conn.sql(query).df()
    return df

def main():
    CACHE_NAME = "naverblog.sqlite"
    CWD = Path.cwd()
    CACHE_PATH = CWD / CACHE_NAME
    if not CACHE_PATH.exists():
        initialize_db(CACHE_PATH)

    driver = initialize_selenium_driver(headless=False)
    ############ Continue from blog_urls #############################
    blog_urls_path = Path(r"G:\My Drive\Data\naver_search_results\naverblog_urls.pkl")
    blog_reviews_path = Path(r"G:\My Drive\Data\naver_search_results\naverblog_reviews.parquet.gzip")
    with open(blog_urls_path, "rb") as rf:
        blog_urls = pickle.load(rf)
    
    try:
        final_blog_reviews = collect_blog_reviews(driver, CACHE_PATH, blog_urls)
        final_blog_reviews.to_parquet(blog_reviews_path, compression="gzip")
    except Exception as e:
        driver.quit()
        raise e
    else:
        driver.quit()
#%%
if __name__ == "__main__":
    # main() 
    pass

#%%
editorversion_to_url_path = Path("G:/My Drive/Data/naver_search_results/editorv_classified_naverblog.pkl")
with open(editorversion_to_url_path, "rb") as rf:
    editorversion_to_url = pickle.load(rf)
# %%
