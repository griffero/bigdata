# -*- coding: utf-8 -*-
import psutil
import mysql.connector
import json
from datetime import datetime
import time
AVAILABLE_MEMORY = 1

cn = mysql.connector.connect(user='root', password='2433850',
                                 host='127.0.0.1',
                                 database='trump',
                                 use_unicode=True)

def get_tweets_by_hour(cursor):
    cursor.execute("SELECT COUNT(*) tweets, EXTRACT(DAY FROM time) AS day, EXTRACT(HOUR FROM time) AS hour FROM trump GROUP BY day, hour ORDER BY day, hour;")
    return cursor.fetchall()

def count_lang_by_country(cursor):
    cursor.execute("SELECT COUNT(*) tweets, lang, country FROM trump WHERE country='US'  GROUP BY country, lang;")
    return cursor.fetchall()

def average_followers_by_country(cursor):
    cursor.execute("SELECT AVG(followers), country FROM trump WHERE country!=''  GROUP BY country;")
    return cursor.fetchall()

cursor = cn.cursor()

print "Begin..."
memory_init = psutil.virtual_memory()[AVAILABLE_MEMORY]
cpu_init = psutil.cpu_percent(interval=0.0)
time_init = time.time()

tweets_by_hour = get_tweets_by_hour(cursor)
lang_by_country = count_lang_by_country(cursor)
average_followers = average_followers_by_country(cursor)

duration = time.time() - time_init
cpu_usage = psutil.cpu_percent(interval=0.0)
memory_usage = (memory_init - psutil.virtual_memory()[AVAILABLE_MEMORY])/1048576
print "End!"
print "________________________________"
print "Query duration: {0}".format(duration)
print "CPU use: {0}".format(cpu_usage)
print "MEMORY use: {0}".format(memory_usage)
cursor.close()
cn.close()
