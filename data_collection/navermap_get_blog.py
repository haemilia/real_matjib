#%%
from pathlib import Path
import pickle
import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from tqdm import tqdm
import re

def get_blog_html(session:requests_cache.CachedSession, url:str)-> bytes|None:
    """Request for blog html content"""
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error while getting {url}: {e}")
    return None

def extract_editor_version(post_property):
    """Extract editorversion information from blog `_post_property`"""
    if post_property and isinstance(post_property, Tag):
        editor_version = post_property.get("editorversion")
        return editor_version
    else:
        return None
    
def extract_blog_info(soup:BeautifulSoup) -> Tuple[Dict,Any]:
    info_container = {}
    post_property = soup.find('div', id='_post_property')
    editorversion = extract_editor_version(post_property)
    info_container["editorversion"] = editorversion
    return info_container, editorversion
    
def check_if_we_can_handle_editorversion(editorversion:str) -> bool:
    if editorversion in ["1",]:
        return True
    else:
        return False
    
def classify_editor_version(session:requests_cache.CachedSession, blog_urls:Dict[str, List[str]]) -> Dict[str, List[str]]:
    """To be used in scraping preparation"""
    editorversion_to_url:Dict[str, List[str]] = defaultdict(list)
    for _, url_list in tqdm(blog_urls.items()):
        for one_url in tqdm(url_list):
            blog_raw = get_blog_html(session, one_url)
            if blog_raw:
                blog_soup = BeautifulSoup(blog_raw, "html.parser")
            else:
                continue
            editor_version = extract_editor_version(blog_soup)
            if isinstance(editor_version, str):
                editorversion_to_url[editor_version].append(one_url)
            else:
                print("Weird value in editor version:", editor_version)

    return editorversion_to_url

def get_post_date(editorversion:str, soup:BeautifulSoup) -> pd.Timestamp:
    
    method = {
        "1": lambda soup: pd.Timestamp(soup.find(class_="se_date").text.strip()),

    }

    post_date = method[editorversion](soup)
    return post_date

def get_text(editorversion:str, soup:BeautifulSoup) -> str:
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
    }

    text = method[editorversion](soup)
    return text

def get_image_url_list(editorversion:str, soup:BeautifulSoup) -> List[str|None] | None:
    def extract_1(soup):
        all_post_images = soup.find(id="viewTypeSelector").find_all("img", class_="fx _postImage")
        large_src_pattern = re.compile("'(https:.*)'")
        def search_for_img_url(img_item):
            if (img_item.attrs.get("largesrc") is None) or (img_item.attrs.get("largesrc") == ""):
                src_url = img_item.attrs.get("src")
            else:
                img_src = img_item.attrs.get("largesrc")
                large_src_search_result = large_src_pattern.search(img_src)
                if large_src_search_result is not None:
                    src_url = large_src_search_result.group(1)
                else:
                    src_url = None
            return src_url
        img_url_list = list(map(search_for_img_url, all_post_images))
        if not img_url_list:
            return None
        return img_url_list
    
    method = {
        "1": extract_1,

    }
    image_url_list = method[editorversion](soup)
    return image_url_list

def get_sticker_url_list(editorversion:str, soup:BeautifulSoup) -> List[str|None] | None:
    def extract_1(soup):
        return None
    method = {
        "1":extract_1,

    }
    sticker_url_list = method[editorversion](soup)
    return sticker_url_list

def get_vidthumb_url_list(editorversion:str, soup:BeautifulSoup) -> List[str|None] | None:
    def extract_1(soup):
        return None
    method = {
        "1":extract_1,

    }
    vidthumb_url_list = method[editorversion](soup)
    return vidthumb_url_list


def main():
    CACHE_NAME = "naverblog"
    cached_session = requests_cache.CachedSession(CACHE_NAME)
    ############ Continue from blog_urls #############################
    blog_urls_path = Path(r"G:\My Drive\Data\naver_search_results\naverblog_urls.pkl")
    with open(blog_urls_path, "rb") as rf:
        blog_urls = pickle.load(rf)
    editorversion_to_url = classify_editor_version(cached_session, blog_urls)

    editorversion_to_url_path = Path("G:/My Drive/Data/naver_search_results/editorv_classified_naverblog.pkl")
    with open(editorversion_to_url_path, "wb") as wf:
        pickle.dump(editorversion_to_url, wf)

if __name__ == "__main__":
    main() 
    # pass

# #%%
# editorversion_to_url_path = Path("G:/My Drive/Data/naver_search_results/editorv_classified_naverblog.pkl")
# with open(editorversion_to_url_path, "rb") as rf:
#     editorversion_to_url = pickle.load(rf)