'''
Credits for levirve  

https://levirve.github.io/blog/2016/dcard-spider-python-package/
https://pypi.org/project/dcard-spider/

Dcard website: https://www.dcard.tw/f/food

Open terminal to install dcard-spider: "pip install dcard-spider"

The following codes use the Dcard library to crawl the data from the Dcard's Food forum,
and export the result as a .json format file.
'''
import time
import json
from dcard import Dcard
import numpy as np
import pandas as pd
import datetime
import re

start = time.time()
print('The Dcard Crawler program starts...')
today = str(datetime.date.today())
today = today.replace('-','_')

dcard = Dcard()
forum = dcard.forums('food') # <= 改''內容可以指定要爬的版
dcard_json = forum.get_metas(num=1000) # <= 改 (num=？) 內容可以指定要爬的篇數，下載後越新的文章排在越前面
dcard_json = dcard_json[::-1]  # <= 下載後越新的文章排在越後面
#export as the .json format
file_name = 'raw_dcard_food{}.json'.format(today)
with open(file_name, 'w') as f: # <= 改‘’內容可以自訂存擋名稱
    json.dump(dcard_json, f)


dcard_food = pd.read_json(file_name,encoding='utf-8')

# Parse the createAt column as a date-time format

date_list = []
year_list = []
month_list = []
day_list = []
time_list = []
weekday_list = []

for i in range(len(dcard_food.index)):
    time_str = dcard_food['createdAt'][i]
    time_str_list = time_str.split('T')
    final_date = time_str_list[0]
    year_time_day_list = final_date.split('-')
    final_year = int(year_time_day_list[0])
    final_month = int(year_time_day_list[1])
    final_day = int(year_time_day_list[2])
    final_date = final_date.replace('-','/')
    final_date = "'"+final_date+"'"
    date_list.append(final_date)
    year_list.append(final_year)
    month_list.append(final_month)
    day_list.append(final_day)
    
    time2 = time_str_list[1] # Because time is used by timing
    time_str_list = time2.split('.')
    final_time = time_str_list[0]
    time_list.append(final_time)
    
    final_date = final_date.replace("'",'')
    date_str_list = final_date.split('/') 
    year = int(date_str_list[0])
    month = int(date_str_list[1])
    day = int(date_str_list[2])
    weekday = int(datetime.datetime(year,month,day).isoweekday())
    weekday_list.append(weekday)

dcard_food['date'] = date_list
dcard_food['year'] = year_list
dcard_food['month'] = month_list
dcard_food['day'] = day_list
dcard_food['time'] = time_list
dcard_food['weekday'] = weekday_list
#################################################
#Clean the content
content_list = []
for i in range(len(dcard_food.index)):
    excerpt = dcard_food['excerpt'][i]
    #clean img links
    excerpt = re.sub('http(s?):([/|.|\w|\s|-])*\.(?:jpg|gif|png)','',excerpt)
    #clean XD
    excerpt = re.sub('XD|～|\n','',excerpt)
    #clean any links
    excerpt = re.sub('^http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+$','',excerpt)
    #clean emojis
    #RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags = re.UNICODE)
    RE_EMOJI = re.compile('(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c[\ud000-\udfff]|\ud83d[\ud000-\udfff]|\ud83e[\ud000-\udfff]|[\U00010000-\U0010ffff])'
                          , flags = re.UNICODE)
    def strip_emoji(text):
        return RE_EMOJI.sub(r'', text)
    excerpt=strip_emoji(excerpt)
    excerpt = re.sub('[ㄅ|ㄆ|ㄇ|ㄈ|ㄉ|ㄊ|ㄋ|ㄌ|ㄍ|ㄎ|ㄏ|ㄐ|ㄑ|ㄒ|ㄓ|ㄔ|ㄕ|ㄖ|ㄗ|ㄘ|ㄙ|ㄧ|ㄨ|ㄩ|ㄚ|ㄛ|ㄜ|ㄝ|ㄞ|ㄟ|ㄠ|ㄡ|ㄢ|ㄣ|ㄤ|ㄥ|ˇ|ˋ|ˊ|˙|！|？|，|．|／|＄|＠|％|︿|＆|＊|（|）|＿|＋|～|~]','',excerpt)
    # Append the clean content to the content_list
    content_list.append(excerpt)

dcard_food['Content'] = content_list
###################################################
# Parse the title column as a clean content
title_list = []
for i in range(len(dcard_food.index)):
    title = dcard_food['title'][i]
    title = strip_emoji(title)
    title_list.append(title)

dcard_food['Title'] = title_list

#Parse the reaction column and assign its count and change name to the corresponding ones

"""
"286f599c-f86a-4932-82f0-f5a06f1eca03": 愛心
"e8e6bc5d-41b0-4129-b134-97507523d7ff": 哈哈
"4b018f48-e184-445f-adf1-fc8e04ba09b9": 驚訝
"011ead16-9b83-4729-9fde-c588920c6c2d": 跪
"aa0d425f-d530-4478-9a77-fe3aedc79eea": 森77
"514c2569-fd53-4d9d-a415-bf0f88e7329f": 嗚嗚
"""

data_parse = {'id':dcard_food['id'], '286f599c-f86a-4932-82f0-f5a06f1eca03': 0, 'e8e6bc5d-41b0-4129-b134-97507523d7ff': 0,
               '4b018f48-e184-445f-adf1-fc8e04ba09b9': 0, '011ead16-9b83-4729-9fde-c588920c6c2d': 0,
               'aa0d425f-d530-4478-9a77-fe3aedc79eea': 0, '514c2569-fd53-4d9d-a415-bf0f88e7329f': 0}


reaction_tb = pd.DataFrame(data=data_parse)


for i in range(len(dcard_food)):
    for j in dcard_food['reactions'][i]:
        reaction_tb[j.get('id')][i] = j.get('count')

new_reaction_tb = reaction_tb.rename(index=str, columns={"286f599c-f86a-4932-82f0-f5a06f1eca03": "heart",
                                                         "e8e6bc5d-41b0-4129-b134-97507523d7ff": "haha",
                                                         "4b018f48-e184-445f-adf1-fc8e04ba09b9": "surprised",
                                                         "011ead16-9b83-4729-9fde-c588920c6c2d": "kneel",
                                                         "aa0d425f-d530-4478-9a77-fe3aedc79eea": "mad",
                                                         "514c2569-fd53-4d9d-a415-bf0f88e7329f": "cry"})

new_reaction_tb = new_reaction_tb.set_index('id')
##############################################
# area 1
school01 = ['國立臺北大學','臺北大學','國立臺灣藝術大學','國立空中大學','輔仁大學','淡江大學','華梵大學','真理大學','馬偕醫學院','法鼓文理學院',
            '明志科技大學','聖約翰科技大學','景文科技大學','東南科技大學','醒吾科技大學','華夏科技大學','致理科技大學','宏國德霖科技大學',
            '台北海洋科技大學','亞東技術學院','黎明技術學院','耕莘健康管理專科學校','臺北基督學院'] #'新北市'
school02 = ['政治大學','國立政治大學','臺灣大學','國立臺灣大學','臺灣師範大學','國立陽明大學','國立陽明大學','陽明大學','國立臺北藝術大學',
          '臺北藝術大學','國立臺北教育大學','臺北教育大學','臺北市立大學','國立臺灣科技大學','臺灣科技大學','國立臺北科技大學','臺北科技大學',
          '國立臺北護理健康大學','國立臺北商業大學','臺灣戲曲學院','東吳大學','中國文化大學','世新大學','銘傳大學','實踐大學','大同大學','臺北醫學大學',
          '康寧大學','中國科技大學','德明財經科技大學','中華科技大學','臺北城市科技大學','馬偕醫護管理專科學校','中華浸信會基督教台灣浸會神學院',
          '台灣神學研究學院'] #'台北市'
school03 = ['國立臺灣海洋大學', '崇右影藝科技大學', '經國管理暨健康學院'] #'基隆市'
school04 = ['國立中央大學','中央大學','國立體育大學','中原大學','長庚大學','元智大學','開南大學','龍華科技大學','健行科技大學','萬能科技大學',
            '長庚科技大學','南亞技術學院','新生醫護管理專科學校'] #'桃園市'
school05 = ['明新科技大學', '大華科技大學'] #'新竹縣'
school06 = ['國立清華大學', '清華大學', '國立交通大學', '交通大學', '中華大學', '玄奘大學', '元培醫事科技大學'] #'新竹市'
# area 2
school07 = ['國立聯合大學', '育達科技大學', '仁德醫護管理專科學校'] #'苗栗縣'
school08 = ['國立彰化師範大學', '大葉大學', '明道大學', '建國科技大學', '中州科技大學'] #'彰化縣'
school09 = ['國立中興大學','國立臺中教育大學','臺中教育大學','國立臺灣體育運動大學','國立勤益科技大學','國立臺中科技大學','東海大學','逢甲大學',
            '靜宜大學','中山醫學大學','中國醫藥大學','亞洲大學','朝陽科技大學','弘光科技大學','嶺東科技大學','中臺科技大學','僑光科技大學',
            '修平科技大學'] #'台中市'
school10 = ['國立雲林科技大學', '國立虎尾科技大學', '環球科技大學'] #'雲林縣'
school11 = ['國立暨南國際大學', '南開科技大學', '一貫道崇德學院'] #'南投縣'
# area 3
school12 = ['國立中山大學','國立高雄師範大學','國立高雄大學','國立高雄餐旅大學','國立高雄科技大學','高雄市立空中大學','義守大學','高雄醫學大學',
            '樹德科技大學','輔英科技大學','正修科技大學','高苑科技大學','文藻外語大學','東方設計大學','和春技術學院','樹人醫護管理專科學校',
            '育英醫護管理專科學校','一貫道天皇學院','國立高雄第一科技大學'] #'高雄市'
school13 = ['國立成功大學','國立臺南藝術大學','國立臺南大學','國立臺南護理專科學校','長榮大學','台灣首府大學','中信金融管理學院','南臺科技大學',
            '崑山科技大學','嘉南藥理大學','台南應用科技大學','遠東科技大學','中華醫事科技大學','南榮科技大學','敏惠醫護管理專科學校',
            '台灣基督長老教會南神神學院'] #'台南市'
school14 = ['國立嘉義大學', '大同技術學院'] #'嘉義市'
school15 = ['國立屏東大學', '屏東大學', '國立屏東科技大學', '大仁科技大學', '美和科技大學', '慈惠醫護管理專科學校'] #'屏東縣'
school16 = ['國立中正大學', '中正大學', '南華大學', '稻江科技暨管理學院', '吳鳳科技大學', '崇仁醫護管理專科學校'] #'嘉義縣'
# area 4
school17 = ['國立宜蘭大學', '佛光大學', '蘭陽技術學院', '聖母醫護管理專科學校'] #'宜蘭縣'
school18 = ['國立臺東大學', '國立臺東專科學校'] #'台東縣'
school19 = ['國立東華大學', '慈濟大學', '慈濟科技大學', '大漢技術學院', '臺灣觀光學院'] #'花蓮縣'
# area 5
school20 = ['國立澎湖科技大學'] #'澎湖縣'
school21 = ['國立金門大學'] #'金門縣'

area_code = []
area_city = []

school_list = list(dcard_food['school'])
for i in school_list:
    # area 1
    if i in school01:
        area_code.append(1)
        area_city.append('新北市')
    elif i in school02:
        area_code.append(1)
        area_city.append('台北市')
    elif i in school03:
        area_code.append(1)
        area_city.append('基隆市')
    elif i in school04:
        area_code.append(1)
        area_city.append('桃園市')
    elif i in school05:
        area_code.append(1)
        area_city.append('新竹縣')
    elif i in school06:
        area_code.append(1)
        area_city.append('新竹市')
    # area 2
    elif i in school07:
        area_code.append(2)
        area_city.append('苗栗縣')
    elif i in school08:
        area_code.append(2)
        area_city.append('彰化縣')
    elif i in school09:
        area_code.append(2)
        area_city.append('台中市')
    elif i in school10:
        area_code.append(2)
        area_city.append('雲林縣')
    elif i in school11:
        area_code.append(2)
        area_city.append('南投縣')
    # area 3
    elif i in school12:
        area_code.append(3)
        area_city.append('高雄市')
    elif i in school13:
        area_code.append(3)
        area_city.append('台南市')
    elif i in school14:
        area_code.append(3)
        area_city.append('嘉義市')
    elif i in school15:
        area_code.append(3)
        area_city.append('屏東縣')
    elif i in school16:
        area_code.append(3)
        area_city.append('嘉義縣')
    # area 4
    elif i in school17:
        area_code.append(4)
        area_city.append('宜蘭縣')
    elif i in school18:
        area_code.append(4)
        area_city.append('台東縣')
    elif i in school19:
        area_code.append(4)
        area_city.append('花蓮縣')
    # area 5
    elif i in school20:
        area_code.append(5)
        area_city.append('澎湖縣')
    elif i in school21:
        area_code.append(5)
        area_city.append('金門縣')
    else:
        area_code.append(0)
        area_city.append('others')
dcard_food['area'] = area_code
dcard_food['city'] = area_city
##############################################

new_topic_list = []

old_topic_list = list(dcard_food['topics'])
for i in old_topic_list:
    item = str(i)
    item = item.replace('[','')
    item = item.replace(']','')
    new_topic_list.append(item) 
dcard_food['topic'] = new_topic_list
##############################################
#Using the old columns to make a new and clean dataframe
data_parse = {'id':dcard_food['id'],'date': dcard_food['date'],'year': dcard_food['year'],'month': dcard_food['month'],
              'day': dcard_food['day'], 'weekday': dcard_food['weekday'],'time': dcard_food['time'],'area':dcard_food['area'],
              'city':dcard_food['city'],'school': dcard_food['school'], 'department': dcard_food['department'],
              'gender': dcard_food['gender'], 'topic': dcard_food['topic'], 'title': dcard_food['Title'], 'content': dcard_food['Content']}

new_dcard_food = pd.DataFrame(data=data_parse)
new_dcard_food = new_dcard_food.set_index('id')

result_db = pd.concat([new_dcard_food, new_reaction_tb], axis=1, join='inner')
result_db = result_db.reset_index()

# Write-out
with open('test_clean_dcard_food{}.json'.format(today), 'w', encoding='utf-8') as file:
        result_db.to_json(file, force_ascii=False, orient='records')
##############################################
# Insert into the database
import mysql.connector

cnx = mysql.connector.connect(user='ray', password='Taiwan#1',
                              host='127.0.0.1',
                              database='dcad_db')
cursor = cnx.cursor()
query = ("SELECT id FROM test02")  # check the id list, and use it as the base to either UPDATE or INSERT 
cursor.execute(query)

id_list =[]

for i in cursor:
    id_list.append(i[0])
    

for i in range(len(result_db)):
    content_list = {'area': int(result_db.iloc[i]['area']),'city': str(result_db.iloc[i]['city']), 
                        'content': str(result_db.iloc[i]['content']),'cry': int(result_db.iloc[i]['cry']),
                        'date': str(result_db.iloc[i]['date']), 'department': str(result_db.iloc[i]['department']),'gender': str(result_db.iloc[i]['gender']),
                        'haha': int(result_db.iloc[i]['haha']),'heart': int(result_db.iloc[i]['heart']),'id': int(result_db.iloc[i]['id']),
                        'kneel': int(result_db.iloc[i]['kneel']),'mad': int(result_db.iloc[i]['mad']),'school': str(result_db.iloc[i]['school']),
                        'surprised': int(result_db.iloc[i]['surprised']),'time': str(result_db.iloc[i]['time']),'title': str(result_db.iloc[i]['title']),
                        'topic': str(result_db.iloc[i]['topic']),'weekday': int(result_db.iloc[i]['weekday']),'year': int(result_db.iloc[i]['year']),
                        'month': int(result_db.iloc[i]['month']),'day': int(result_db.iloc[i]['day'])}
    if result_db.iloc[i]['id'] in id_list: # Update  
        #Insert into Database
        update_article = "UPDATE test02 SET date = %(date)s, year = %(year)s, month = %(month)s, day = %(day)s, cry = %(cry)s, haha = %(haha)s, heart = %(heart)s, kneel = %(kneel)s, mad = %(mad)s, surprised = %(surprised)s WHERE id = %(id)s"
        # Insert new article
        cursor.execute(update_article,content_list)
        # Make sure data is committed to the database
        cnx.commit()
        print(i,":",'Updated the database.')
    else: # Insert    
        #Insert into Database
        add_article = ("INSERT INTO test02"
                       "(area,city,content,cry,date, department,gender,haha,heart,id,kneel,mad,school,surprised,time,title,topic,weekday,year,month,day) "
                       "VALUES (%(area)s, %(city)s, %(content)s,%(cry)s,%(date)s,%(department)s,%(gender)s,%(haha)s,%(heart)s,%(id)s,%(kneel)s,%(mad)s,%(school)s,%(surprised)s,%(time)s,%(title)s,%(topic)s,%(weekday)s,%(year)s,%(month)s,%(day)s)")
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
print('Finished')
print('Total time:', minute, 'm', second, 's')