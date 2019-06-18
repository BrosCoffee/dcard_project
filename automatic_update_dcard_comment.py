import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import time
import re
from collections import OrderedDict
import mysql.connector
import datetime

today = str(datetime.date.today())
today = today.replace('-','_')
file_name = 'clean_dcard_food{}.json'.format(today)
dcard_food = pd.read_json(file_name,encoding='utf-8')

assign_list = dcard_food['id']
list1 = []
list2 = []

start = time.time()
print('The program starts...')
count = len(assign_list)
article_count = 0
for i in assign_list:
    count = count - 1
    print(count, '...')
    time.sleep(2)
    dcard_url = 'https://www.dcard.tw/f/food/p/' + str(i)
    response = requests.get(dcard_url)
    soup = BeautifulSoup(response.text)
    comment = soup.find_all(attrs={'CommentEntry_content_1ATrw1'})
    # Delete the duplications
    all_comment = list(set([j.text for j in comment]))
    # Assing a empty string to store the clean items from the list, and make all as a long list
    clean_comment = ''

    # Parse each element in the data list
    for item in all_comment:
        # clean img links
        item = re.sub('http(s?):([/|.|\w|\s|-])*\.(?:jpg|gif|png)', '', item)
        # clean http(s) links
        item = re.sub(
            r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\
            .[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$',
            '', item)
        # clean XD and so on
        item = re.sub('.~|^_^|￣^￣|XDD|ಠ_ಠ|๑´ڡ`๑*|｡･ω･｡|˶᷄ ̫ ᷅˵|;´༎ຶД༎ຶ`', '', item)
        item = re.sub('[ㄅ|ㄆ|ㄇ|ㄈ|ㄉ|ㄊ|ㄋ|ㄌ|ㄍ|ㄎ|ㄏ|ㄐ|ㄑ|ㄒ|ㄓ|ㄔ|ㄕ|ㄖ|ㄗ|ㄘ|ㄙ|ㄧ|ㄨ|ㄩ|ㄚ|ㄛ|ㄜ|ㄝ|ㄞ|ㄟ|ㄠ|ㄡ|ㄢ|ㄣ|ㄤ|ㄥ|ˇ|ˋ|ˊ|˙|！|？|，|．|／|＄|＠|％|︿|＆|＊|（|）|＿|＋|～|~]','',item)
        # clean '已經刪除的內容就像 Dcard 一樣，錯過是無法再相見的！'
        item = re.sub('已經刪除的內容就像 Dcard 一樣，錯過是無法再相見的！', '', item)
        # clean any links
        #item = re.sub('.http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+$', '', item)
        item = re.sub('(?:(?:https?|ftp|file):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])', '', item)
        # clean emojis
        RE_EMOJI = re.compile(
            '(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c[\ud000-\udfff]|\ud83d[\ud000-\udfff]|\ud83e[\ud000-\udfff]|[\U00010000-\U0010ffff])'
            , flags=re.UNICODE)


        def strip_emoji(text):
            return RE_EMOJI.sub(r'', text)


        item = strip_emoji(item)
        clean_comment += item
        # id_comment_dict['Comment'] = clean_comment
    list1.append(clean_comment)
    list2.append(i)

data_parse = OrderedDict([('ID', list2), ('Comment', list1)])
dcard_food_df = pd.DataFrame(data_parse)

with open('dcard_food_comment{}.json'.format(today), 'w', encoding='utf-8') as file:
    dcard_food_df.to_json(file, force_ascii=False, orient='records')

##############################################

cnx = mysql.connector.connect(user='ray', password='Taiwan#1',
                              host='127.0.0.1',
                              database='dcad_db')
cursor = cnx.cursor()
query = ("SELECT id FROM test02")
cursor.execute(query)

id_list =[]

for i in cursor:
    id_list.append(i[0])
    

dcard = pd.read_json('dcard_food_comment{}.json'.format(today),encoding='utf-8')

for i in range(len(dcard)):
    content_list = {'comment': str(dcard.iloc[i]['Comment']),'id': int(dcard.iloc[i]['ID'])} 
    if dcard.iloc[i]['ID'] in id_list: # Update   
        #Insert into Database
        update_article = "UPDATE test02 SET comment = %(comment)s WHERE id = %(id)s"
        # Insert new article
        cursor.execute(update_article,content_list)
        # Make sure data is committed to the database
        cnx.commit()
        print(i,":",'Updated the database.')
    else: # Insert
        #Insert into Database
        add_article = ("INSERT INTO test02"
                       "(id, comment)"
                       "VALUES (%(id)s,%(comment)s)")
        # Insert new article
        cursor.execute(add_article,content_list)
        # Make sure data is committed to the database
        cnx.commit()
        print(i,":",'Inserted into the database.')
        
cursor.close()
cnx.close()
##############################################
end = time.time()
minute = round((end - start) / 60)
second = round((end - start) % 60)
# print(dcard_food_df.head())  # show the front rows
print('Finished')
print('Total time:', minute, 'm', second, 's')