# -*- coding: utf-8 -*-
import mysql.connector
import json
from datetime import datetime

cn = mysql.connector.connect(user='root', password='2433850',
                                 host='127.0.0.1',
                                 database='trump',
                                 use_unicode=True)

cursor = cn.cursor()
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")

fo = open("trump.json", "r")
lines = fo.readlines()
for line in lines:
    tweet = json.loads(line)
    if 'text' in tweet:
        text = tweet['text']
        time = datetime.fromtimestamp(int(tweet['timestamp_ms'])/1000)
        lang =  tweet['lang']
        followers =  int(tweet['user']['followers_count'])
        country = ""
        city = ""
        if tweet['place'] != None:
            country = tweet['place']['country_code']
            city = tweet['place']['name']
        add_tweet = ("INSERT INTO trump "
               "(text, time, lang, followers, country, city) "
               "VALUES (%s, %s, %s, %s, %s, %s)")
        data_tweet = [text, time, lang, followers, country, city]
        cursor.execute(add_tweet, data_tweet)
        cn.commit()

cursor.close()
cn.close()
