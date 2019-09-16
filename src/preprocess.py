# -*- coding:utf-8 -*-
import sys
import json
import time

import pandas as pd
from requests_oauthlib import OAuth1Session
from collections import defaultdict
from tqdm import tqdm
from multiprocessing import Pool
import multiprocessing as multi
import pickle

import config

TWEETPATH = 'data/twitter/tweets_open.csv'
CK = config.CONSUMER_KEY
CS = config.CONSUMER_SECRET
AT = config.ACCESS_TOKEN
ATS = config.ACCESS_TOKEN_SECRET
twitter = OAuth1Session(CK, CS, AT, ATS)
url = "https://api.twitter.com/1.1/statuses/show.json"

def load_tweetdf():
    df_tweetid = pd.read_csv( TWEETPATH, header=None, sep=',' )
    df_tweetid.columns =\
        ['id', 'genre', 'status_id', '4', '5', '6', '7', '8']
    print(len(df_tweetid))
    df_tweetid_dropna = df_tweetid.dropna()
    df_tweetid_dropna = df_tweetid_dropna.astype(int)
    print(len(df_tweetid_dropna))
    return df_tweetid_dropna.query('genre==10000')
    #return df_tweetid_dropna

def getTweet(statusIds):
    #tweet_dict = defaultdict(str)
    limit = 900
    texts = []
    with Pool(multi.cpu_count()-1) as p:
        for i in range(0, len(statusIds), limit):
            sids = statusIds[i:i+limit]
            imap = p.imap(getTweetsByStatusId, sids)
            texts += list(tqdm(imap, total=len(sids)))
            print(texts)
            time.sleep(960)

    with open('data/twitter/texts.pickle', mode='wb') as f:
        pickle.dump(texts, f)
    return texts

def getTweetsByStatusId( n ):
    params ={'id':n}
    req = twitter.get(url, params=params)
    if req.status_code==200:
        timeline = json.loads(req.text)
        return timeline['text']
    else:
        return 'no data'

def main():
    df_tweetid = load_tweetdf()
    statusIds = df_tweetid['status_id'].values

    tweet_dict = getTweet( statusIds )
    df_tweettext = pd.DataFrame(tweet_dict)
    print( df_tweettext.head() )

    sys.exit()
    df_tweetid.merge(df_tweettext, how='inner', on='status_id')

if __name__ == "__main__":
    main()
