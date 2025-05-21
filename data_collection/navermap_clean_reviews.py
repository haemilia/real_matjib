# Check out the collected data
#%%
import pandas as pd
import numpy as np
from pathlib import Path
import pickle

OUTPUT_DIR = Path('G:/My Drive/Data/naver_search_results')
OUTPUT_DIR.mkdir(exist_ok=True)

with open(OUTPUT_DIR/"mapogu_yeonnamdong_naver_reviews_final.pkl", "rb") as rf:
    reviews = pickle.load(rf)
#%%
