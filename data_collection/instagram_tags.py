# 참고사이트
# [인스타 크롤링(1)](https://blog.naver.com/cygnet3rd/222885102852),
# [인스타 크롤링(2)](https://velog.io/@leeug9002/python-selenium%EC%9D%B8%EC%8A%A4%ED%83%80-%ED%81%AC%EB%A1%A4%EB%A7%81),
# [인스타 게시물 내 사진 크롤링](https://proefforter.tistory.com/36),
# [리스트 중복 제거&순서 유지](https://m31phy.tistory.com/130)

from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

import requests
from re import escape
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

import urllib.request
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup, NavigableString, Tag

import os
import shutil   #폴더를 zip으로 묶기
import requests #이미지 다운로드용
from pathlib import Path

# 데이터 수집: 아이디, 본문 내용, 해시태그, 댓글, 좋아요 수, 작성 날짜, 위치
def get_content(driver):
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    time.sleep(2)

    #아이디
    ids = soup.select('span.xjp7ctv')[1].text

    time.sleep(2)

    #content
    h1 = soup.select('h1._ap3a._aaco._aacu._aacx._aad7._aade')
    text_before_a = str(h1).split('<a')[0]

    if len(BeautifulSoup(text_before_a, 'html.parser').text) == 1:
        text = ''
    else:
        text = BeautifulSoup(text_before_a, 'html.parser').text[1:-1]  #[, ] 제거

    time.sleep(2)

    #tags
    tags_ln = len(soup.select('h1._ap3a._aaco._aacu._aacx._aad7._aade > a'))

    if tags_ln > 0:
        hashtags = soup.select('h1._ap3a._aaco._aacu._aacx._aad7._aade > a')
        tags = []

        for i in range(tags_ln):
            hashtag = hashtags[i].text
            tags.append(hashtag)
    else:
        tags = ''
    time.sleep(2)

    #댓글
    cmts_ln = len(soup.select('div.xt0psk2 > span'))

    if cmts_ln > 0:
        comments = soup.select('div.xt0psk2 > span')
        cmts = []

        for i in range(cmts_ln-1):
            comment = comments[i].text
            cmts.append(comment)

    else:
        cmts = ''

    time.sleep(2)

    #좋아요
    likes_tag = soup.select('section.x12nagc.x182iqb8.xv54qhq.xf7dkkf > div > div > span > a > span > span')

    if likes_tag:
        likes = likes_tag[0].text
    else:
        #'여러 명'이 좋아할 경우 또는 조회수 언급 등 기타
        likes = '0'

    time.sleep(2)

    #게시 날짜
    date = soup.select('time.x1p4m5qa')[0]['datetime'][:10]

    time.sleep(2)

    #장소
    try:
        place = soup.select('div._ap3a._aacn._aacu._aacy._aada._aade')[0].text
    except:
        place = ''

    time.sleep(2)

    data = [ids, text, tags, cmts, likes, date, place]
    return data

#포스트별 이미지 다운로드 받고 담을 폴더 생성
def save_imgzipfolder(driver, j, clean):
    time.sleep(2)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    #time.sleep(8)
    
    #각 포스트 이미지 담을 폴더 생성
    folder_name = f'{clean}_img_folder{j+1}'
    save_dir = Path('py_instagram_imgs') / folder_name
    save_dir.mkdir(parents=True, exist_ok=True) #폴더 생성

    #time.sleep(8)

    #이미지 링크
    test_img_urls = []
    img_ln = len(soup.select('div._acnb'))  #이미지 2개 이상일 때 동그라미 개수

    if img_ln > 0:
        for i in range(img_ln):
            img_containers = driver.find_elements(By.CSS_SELECTOR, 'li._acaz')
            img_ln2 = len(img_containers)
            for j in range(img_ln2):
                try:
                    img = img_containers[j].find_element(By.TAG_NAME, 'img')
                    img_src = img.get_attribute('src')
                    test_img_urls.append(img_src)
                except:
                    pass
            time.sleep(2)
            if i != img_ln-1:
                driver.find_element(By.CLASS_NAME,"_afxw._al46._al47").click()  #다음 사진으로 이동
    else:
        img = driver.find_elements(By.CSS_SELECTOR, 'div._aagu > div > img')[j]
        img_src = img.get_attribute('src')
        test_img_urls.append(img_src)

    img_urls = list(dict.fromkeys(test_img_urls))   #이미지 중복 제거, 원래 담았던 순서대로 정렬

    #게시물 아이디
    ids = soup.select('span.xjp7ctv')[1].text

    #수집한 이미지 다운로드
    for k in range(len(img_urls)):
        filename = '{}_{}_img{}.jpg'.format(folder_name, ids, k+1)
        file_path = save_dir / filename
        try:
            resp = requests.get(img_urls[k], stream=True, timeout=15)
            resp.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
        except Exception as e:
            print(f'다운도드 실패')

# 크롬 브라우저 열고 인스타그램 접속
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

url = 'https://www.instagram.com'

driver.get(url)

time.sleep(2)

# **인스타그램 로그인**
# - `By.NAME`: HTML 태그 중 `name='...'` 속성을 기준으로 찾겠다
# - ex. `<input t ype='password' name='password'>`
userId = 'id'
input_id = driver.find_element(By.NAME, 'username')
input_id.clear()
input_id.send_keys(userId)

userPw = 'password'
input_pw = driver.find_element(By.NAME, 'password')
input_pw.clear()
input_pw.send_keys(userPw)
input_pw.submit()

# **연남동 일반 음식점 리스트(구글 기준) 데이터셋**
import pandas as pd

df = pd.read_csv('yeonnam_restaurant_reviews_googlemaps.csv')

# - 사업장명(가게 이름) 중복 방지
house_list = list(df['사업장명'].unique())

#해시태그는 띄어쓰기 X
for i in range(len(house_list)):
    house_list[i] = house_list[i].replace(' ', '')    #문자열 내부 공백 제거
#print(len(house_list)) #630

# - 잘 되고 있는지 원활한 점검을 위해 10개씩 끊기
exp_house_lists = {}

for i in range(1, 64):
    start = (i-1) * 10
    end = i*10

    exp_house_lists[i] = house_list[start:end]  #house_list 10개씩 자르기

results = {}
result_df = {}

time.sleep(5)

#포스트 폴더 담을 폴더 생성
img_folder_name = 'py_instagram_imgs'
base_dir = Path(img_folder_name)
os.makedirs(img_folder_name, exist_ok=True) #폴더 생성

time.sleep(5)

for i in range(1, 64):
    results[i] = []

    for exp in exp_house_lists[i]:
        #검색창 버튼 클릭
        search_btn = driver.find_elements(By.CSS_SELECTOR, 'div.x1iyjqo2.xh8yej3 > div')[1]
        search_btn.click()

        time.sleep(7)

        #검색할 해시태그
        no_parens = re.sub(r'\(.*?\)', '', exp) #1단계: 괄호와 그 안의 내용 제거
        clean = re.sub(r'[^가-힣a-zA-Z]', '', no_parens)    #2단계: 한글, 영문 외의 모든 문자 제거
        search = '#' + clean
        search_input = driver.find_element(By.CSS_SELECTOR, 'div.xjoudau.x6s0dn4.x78zum5.xdt5ytf.x1c4vz4f.xs83m0k.xrf2nzk.x1n2onr6.xh8yej3.x1hq5gj4 > input')
        #해시태그 검색창에 넣기
        search_input.send_keys(search) #html의 input 태그로 전송

        time.sleep(7)

        #검색 시 해시태그 존재 O
        try:
            #첫 번째 해시태그 추출
            first_tag_name = driver.find_element(By.CSS_SELECTOR, 'span.x1lliihq.x193iq5w.x6ikm8r.x10wlt62.xlyipyv.xuxw1ft').text
            tag_name = first_tag_name.lstrip('#')   #해시태그 지우기
            time.sleep(3)
            #첫 번째 해시태그 클릭
            go_first_tag = driver.find_element(By.CSS_SELECTOR, 'div.x9f619.x78zum5.xdt5ytf.x1iyjqo2.x6ikm8r.x1odjw0f.xh8yej3.xocp1fn > a')
            #ActionChains(driver).move_to_element(go_first_tag).click().perform()
            go_first_tag.click()

            time.sleep(8)

            #첫 번째 게시물 클릭: 만약 검색한 사업장명이 그대로 안 나올 경우 연관검색어 순위가 가장 높은 걸로 검색
            go_first_post = driver.find_element(By.CSS_SELECTOR, 'div._aagw')
            time.sleep(3)
            go_first_post.click()   #검색 후 첫 번째 게시물 강제 클릭

            time.sleep(5) 
            
            post_ln = len(driver.find_elements(By.CSS_SELECTOR, 'div._aagw')) -1
            target = 10 #스크롤링할 게시물 개수

            if post_ln >= target:
                for j in range(target):
                    data = get_content(driver)
                    time.sleep(5)
                    save_imgzipfolder(driver, j ,clean)
                    time.sleep(5)
                    data.insert(0, exp)   #첫 번째 자리에 '사업장명' 추가
                    data.insert(1, tag_name)    #검색한 태그 네임 추가
                    results[i].append(data)
                    time.sleep(3)
                    if i != target-1:
                        #다음 게시물로 이동
                        go_nxt_post = driver.find_elements(By.CSS_SELECTOR, 'div._aaqg._aaqh > button')[0]
                        time.sleep(5)
                        go_nxt_post.click()
                    time.sleep(5)
            else:
                for j in range(post_ln):
                    data = get_content(driver)
                    time.sleep(5)
                    save_imgzipfolder(driver, j ,clean)
                    time.sleep(5)
                    data.insert(0, exp)    #첫 번째 자리에 '사업장명' 추가
                    data.insert(1, tag_name)    #검색한 태그 네임 추가
                    results[i].append(data)
                    time.sleep(3)
                    if i != post_ln-1:
                        #다음 게시물로 이동
                        go_nxt_post = driver.find_elements(By.CSS_SELECTOR, 'div._aaqg._aaqh > button')[0]
                        time.sleep(5)
                        go_nxt_post.click()
                    time.sleep(5)

        #검색 시 해시태그 존재 X
        except Exception as e:
            print(exp)
            print(e)

        #홈 화면으로 이동
        home = 'https://www.instagram.com/'
        driver.get(home)
        time.sleep(5)

    result_df[i] = pd.DataFrame(results[i])
    result_df[i].columns = ['food_house', 'search', 'ids', 'text', 'tags', 'cmts', 'likes', 'date', 'place']
    result_df[i].to_excel(f'result_df{i}.xlsx', index=False, engine='openpyxl')

    time.sleep(5)

# 데이터프레임 병합
df = pd.concat(
    [pd.read_excel(f'result_df{i}.xlsx') for i in range(1, 64)],
    axis=0,
    ignore_index=True
)

df.to_excel('instagram_py.xlsx', index=False, engine='openpyxl')