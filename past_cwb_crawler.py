
import csv
import os
import datetime
import numpy as np
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
import pymysql


file_path = r'D:\open_data\weather_past_obs'
#file_path = r'C:\isvms\weather_past_obs'
cwb_data = "cwb_weather_past_pre"
if not os.path.exists(file_path+'\\'+cwb_data):
    os.mkdir(file_path+'\\'+cwb_data)


begin_date = datetime.date(2016, 5, 23)
end_date = datetime.date(2019, 3, 11)
conv2num = True 

def GetWeather(date, station_id, station_name):
    #兩次URL編碼
    station_name = urllib.parse.quote(urllib.parse.quote(station_name))
    url = "http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station="+station_id+"&stname="+station_name+"&datepicker="+date
    req = urllib.request.Request(url)
    f = urllib.request.urlopen(req)
    soup = BeautifulSoup(f.read().decode('utf-8','ignore'), "lxml")
    data = soup.find_all('tr')
    del data[0]
    del data[0]
    del data[0]
    data_dict = {}
    for ix,tr in  enumerate(data):
        if ix in ([6,12,18,24]):
            data_list = []
            for idx,td in enumerate(tr.findAll('td')):
                tr_text = td.get_text().rstrip()
                if idx == 0:
                    hour = int(tr_text)
                else:
                    if tr_text == "":
                        data_list.append(None)
                    else:
                        if conv2num:
                            if tr_text == "T":
                                data_list.append(0.05) #有雨跡，降雨量小於0.01
                            elif tr_text == "X":
                                data_list.append(0) #記錄錯誤
                            elif tr_text == "V":
                                data_list.append(0) #風向不定
                            elif tr_text == "/" :
                                data_list.append(0) #風向不定
                            elif tr_text == "...":
                                data_list.append(None)
                            else:
                                data_list.append(float(tr_text))

                        else:
                            try:
                                tr_text = float(tr_text)
                                data_list.append(tr_text)
                            except:
                                data_list.append(tr_text)


                data_dict[hour] = data_list
    return data_dict

station = pd.read_csv(r'D:\open_data\weather_past_obs\station.csv', encoding='utf-8')
infos = station[['Id','Name','City']].values
for i in range((end_date - begin_date).days+1):
    df_index = 0
    headers = ["station_city", "station_name", "Date", "ObsTime", "StnPres", "SeaPres", "Temperature", "Tddewpoint", 
               "RH", "WS", "WD", "WSGust", "WDGust", "Precp", "PrecpHour", "SunShine", "GloblRad", "Visb", "UVI", "Cloud_Amoun"]
    df = pd.DataFrame(index=np.arange(0,4*len(infos)), columns=headers)
    data_correct = True
    for info in infos:
        station_id = str(info[0])
        station_name = info[1]
        station_city = info[2]
        day = begin_date + datetime.timedelta(days=i)  
        date = str(day)
        weather_dict = GetWeather(date,station_id,station_name)
        if weather_dict == {}:
            print(station_id+station_name+"Error!")
            data_correct = False
            pass
        else:
            for h in ([6,12,18,24]):
                rows = weather_dict[h]
                rows.insert(0,h)
                rows.insert(0,date)
                rows.insert(0,station_name)
                rows.insert(0,station_city)
                df.loc[df_index] = rows
                df_index += 1
     
    if data_correct :
        df.to_csv(file_path+'\\'+cwb_data+'/'+date+'_past_obs'+".csv", encoding="utf-8-sig", index=False)
        listdata = df.values.tolist()
        db = pymysql.connect('IP', 'ACCOUNT', 'PASSWORD', 'DATABASE' ) 
        cursor = db.cursor()
        insertsql = 'insert into weather_past_obs(station_city, station_name, Date, ObsTime, StnPres, SeaPres, Temperature, Tddewpoint, RH, WS, WD, WSGust, WDGust, Precp, PrecpHour, SunShine, GloblRad, Visb, UVI, Cloud_Amoun) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)'
        cursor.executemany(insertsql, listdata)
        db.commit()
        cursor.close()
        db.close()

