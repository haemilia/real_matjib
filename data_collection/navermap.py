#%%
import pandas as pd
import numpy as np
from pathlib import Path

def prepare_restaurant_list(mapogu_path =Path("../dataset/seoul_mapogu_general_restaurants.xlsx")):
    ## Prepare list of restaurants
    dtype = {'도로명우편번호': str}
    mapogu = pd.read_excel("../dataset/seoul_mapogu_general_restaurants.xlsx", dtype=dtype)
    focus_region = "연남동"
    mapogu = mapogu.dropna(subset = ["소재지전체주소", "도로명전체주소"])
    region_df = mapogu[mapogu["도로명전체주소"].str.contains(focus_region)]
    region_df = region_df.dropna(how="all", axis=1)
    return region_df


#%%
region_df = prepare_restaurant_list()
search_info = region_df[["사업장명", "도로명우편번호"]].dropna(how="any")
search_info["도로명우편번호"] = search_info["도로명우편번호"].astype(int)

#%%
# Search for restaurant
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import urllib
from time import sleep


chrome_options = webdriver.ChromeOptions() # when there's a special option, add it here
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

to_search = region_df["사업장명"].sample(20, random_state=42)
#%%
################# Functions to be used during selenium crawling ###########################
def is_there_only_one_answer(driver:webdriver):
    """Returns True if there is only one search result.

    Args:
        driver : (webdriver)
            The selenium webdriver of the current state
    """
    cur_url = driver.current_url
    return "isCorrectAnswer" in urllib.parse.urlparse(cur_url).query
def switch_searchframe(driver:webdriver):
    """Moves to `searchIframe`. Only to be used when inside another iframe."""
    driver.switch_to.parent_frame()
    iframe = driver.find_element(By.ID, "searchIframe")
    driver.switch_to.frame(iframe)
    
def switch_entryframe(drvier:webdriver):
    """Moves to `entryIframe`. Only to be used when inside another iframe."""
    driver.switch_to.parent_frame()
    iframe = driver.find_element(By.ID, "entryIframe")
    driver.switch_to.frame(iframe)



###################### TO BE COMPLETED ####################################################
#TODO: Check if this works; Write function docstring
def is_next_page_disabled(driver):
    """"""
    # Assume driver is inside searchIframe
    pages_box = driver.find_element(By.XPATH,'//*[@id="app-root"]/div/div/div[2]')
    next_page_disabled = pages_box.find_elements(By.TAG_NAME, "a")[-1].get_attribute("aria-disabled")
    if next_page_disabled == "true":
        return True
    else:
        return False

def check_address_in_entryframe(driver:webdriver) -> bool:
    """Check if address in `entryIframe` is in desired region"""
    return True # for now

def collect_place_id(driver) -> str:
    parsed = urllib.parse.urlparse(driver.current_url)
    restaurant_id = parsed.path.split("/")[-1]
    return restaurant_id

def collect_reviews():
    pass
#%%
########################### NEED TO DEBUG!!!!!!!!!! #######################################

for searching_restaurant in to_search:
    ########### Search for restaurant #################
    print(f"Beginning search for {searching_restaurant}")
    url = f"https://map.naver.com/p/search/{'연남 ' +  searching_restaurant}?c=15.00,0,0,0,dh"
    driver.get(url)
    driver.implicitly_wait(time_to_wait=10)
    sleep(1)
    if is_there_only_one_answer(driver):
        print("There's only one restaurant result! Yay!")
        #### Actions for when there is only one answer
        #### Can only check detail page. Check 지번 address for 연남.
        #### If in 연남, collect reviews.
        #### Else, move on to next restaurant
        continue # for now
    else:
        search_result_iframe = driver.find_element(By.ID, "searchIframe")
        driver.switch_to.frame(search_result_iframe)
        try:
            scrollable_box = driver.find_element(By.ID, "_pcmap_list_scroll_container")
        except:
            print("Can't find scrollable")
            continue
        else:
            ### Scroll to the bottom of the search results
            scrollable_box = driver.find_element(By.ID, "_pcmap_list_scroll_container")
            last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_box)

            for i in range(5):
                print("scrolling...")
                # scroll down 600 px 5 times
                driver.execute_script("arguments[0].scrollTop += 600;", scrollable_box)

                # wait for page to load
                sleep(1)

            ### Look through all search result items for place in 연남
            for one_result in scrollable_box.find_elements(By.TAG_NAME, "li"):
                print("looking through search results")
                try:
                    result_area = one_result.find_element(By.XPATH, "div[1]/div[2]/div[3]/div/span[2]/a/span[1]").text
                except:
                    result_area = one_result.find_element(By.XPATH, "div[1]/div[2]/div[4]/div/span[2]/a/span[1]").text
                if "연남" in result_area:
                    thisone = one_result
                    break
                else:
                    thisone = False
            if not thisone:
                ### Can't find restaurant even after searching
                print("Can't find restaurant")
            else:
                ##### See detail page of that search result item
                thisone.find_element(By.XPATH,"div[1]/div[2]/div[1]").click()
                sleep(1)
                thisone.find_element(By.XPATH,"div[1]/div[2]/div[1]").click()
                sleep(2)
                parsed = urllib.parse.urlparse(driver.current_url)
                restaurant_id = parsed.path.split("/")[-1]
                print("The restaurant's id is: ", restaurant_id)
driver.close()
#%%
########## ONE ITEM AREA FOR DEBUGGING ####################################################################################
import re
searching_restaurant = "질리(Gilli)"
searching_restaurant = re.sub(r'\(.*?\)', '', searching_restaurant)
print(f"Beginning search for {searching_restaurant}")
if "연남" in searching_restaurant:
    url = f"https://map.naver.com/p/search/{searching_restaurant}?c=15.00,0,0,0,dh"
else:
    url = f"https://map.naver.com/p/search/{'연남 ' + searching_restaurant}?c=15.00,0,0,0,dh"
driver.get(url)
driver.implicitly_wait(time_to_wait=10)
sleep(2)
if is_there_only_one_answer(driver):
    print("There's only one restaurant result! Yay!")
    #### Actions for when there is only one answer
    #### Can only check detail page. Check 지번 address for 연남.
    #### If in 연남, collect reviews.
    if check_address_in_entryframe(driver):
        restaurant_id = collect_place_id(driver)
        print("The restaurant's id is: ", restaurant_id)
    #### Else, move on to next restaurant
    else:
        print("Bye.")
else:
    search_result_iframe = driver.find_element(By.ID, "searchIframe")
    driver.switch_to.frame(search_result_iframe)
    try:
        scrollable_box = driver.find_element(By.ID, "_pcmap_list_scroll_container")
    except:
        print("Can't find scrollable")
    else:
        ### Scroll to the bottom of the search results
        scrollable_box = driver.find_element(By.ID, "_pcmap_list_scroll_container")
        last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_box)

        for i in range(5):
            print("scrolling...")
            # scroll down 600 px 5 times
            driver.execute_script("arguments[0].scrollTop += 600;", scrollable_box)

            # wait for page to load
            sleep(1)

        ### Look through all search result items for place in 연남
        for one_result in scrollable_box.find_elements(By.TAG_NAME, "li"):
            print("looking through search results")
            try:
                result_area = one_result.find_element(By.XPATH, "div[1]/div[2]/div[3]/div/span[2]/a/span[1]").text
            except:
                result_area = one_result.find_element(By.XPATH, "div[1]/div[2]/div[4]/div/span[2]/a/span[1]").text
            if "연남" in result_area:
                thisone = one_result
                break
            else:
                thisone = False
        if not thisone:
            ### Can't find restaurant even after searching
            print("Can't find restaurant")
        else:
            ##### See detail page of that search result item
            thisone.find_element(By.XPATH,"div[1]/div[2]/div[1]").click()
            sleep(1)
            thisone.find_element(By.XPATH,"div[1]/div[2]/div[1]").click()
            sleep(2)
            restaurant_id = collect_place_id(driver)
            print("The restaurant's id is: ", restaurant_id)
# driver.close()
#%%
dtype_spec = {'도로명우편번호': str}
mapogu = pd.read_excel("../dataset/seoul_mapogu_general_restaurants.xlsx", dtype=dtype_spec)
focus_region = "연남동"
mapogu = mapogu.dropna(subset = ["소재지전체주소", "도로명전체주소"])
region_df = mapogu[mapogu["도로명전체주소"].str.contains(focus_region)]
region_df = region_df.dropna(how="all", axis=1)

region_df["도로명우편번호"]

