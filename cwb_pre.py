
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import urllib
from bs4 import BeautifulSoup
import datetime
import os
import zipfile 
import pymysql


# In[2]:



file_path = r'D:\open_data\weather_predict'
#file_path = r'C:\isvms\predict_cwb'
cwb_data = "cwb_weather_data_pre"
if not os.path.exists(file_path+'\\'+cwb_data):
    os.mkdir(file_path+'\\'+cwb_data)


# In[3]:


url='https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={formatt}'
dataid='F-D0047-093'
apikey='CWB-D931EFB8-3C85-4845-869E-665B13359FC6'
formatt='zip'
link=url.format(dataid=dataid,apikey=apikey,formatt=formatt)
data_path=file_path+'\\'+cwb_data+'\\'+dataid+'.'+formatt


# In[4]:


def auto_down(url,filename):
    try:
        urllib.request.urlretrieve(url,filename)
    except :
        print('Download error!!! Reloading now.')
        auto_down(url,filename)


auto_down(link,data_path)
f=zipfile.ZipFile(data_path)


# In[5]:


file = ['63_72hr_CH.xml','64_72hr_CH.xml','65_72hr_CH.xml','66_72hr_CH.xml','67_72hr_CH.xml','68_72hr_CH.xml',
        '09007_72hr_CH.xml','09020_72hr_CH.xml','10002_72hr_CH.xml','10004_72hr_CH.xml','10005_72hr_CH.xml',
        '10007_72hr_CH.xml','10008_72hr_CH.xml','10009_72hr_CH.xml','10010_72hr_CH.xml','10013_72hr_CH.xml',
        '10014_72hr_CH.xml','10015_72hr_CH.xml','10016_72hr_CH.xml','10017_72hr_CH.xml','10018_72hr_CH.xml',
        '10020_72hr_CH.xml']


# In[6]:


today=str(datetime.date.today())


# In[13]:


CITY = []
DISTRICT = []
GEOCODE = []
DAY = []
HOUR = []
DAYTIME=[]
T = []
TD = []
WD = []
WS = []
BF = []
AT = []
Wx = []
Wx_n = []
PoP6h = []
PoP12h = []
get_day = []
RH = []

for filename in file:

    data=f.read(filename).decode('utf-8')
    soup=BeautifulSoup(data,'xml')
    city=soup.locationsName.text
    dists_list=soup.find_all('location')
    for dist_wea in dists_list:
        district=dist_wea.find_all("locationName")[0].text
        geocode = dist_wea.geocode.text
        weather = dist_wea.find_all("weatherElement")
        times=weather[1].find_all('dataTime')
        for tim in times:
            day,time=tim.text.split("T")
            DAY.append(day)
            time_=time.split("+")[0]
            HOUR.append(time_[:2])
            DAYTIME.append(str(day)+' '+str(time_))
            CITY.append(city)
            DISTRICT.append(district)
            GEOCODE.append(geocode)
            get_day.append(today)
        for t in weather[0].find_all("value"):
            T.append(t.text)
        for td  in weather[1].find_all("value"):
            TD.append(td.text)
        for rh  in weather[2].find_all("value"):
            RH.append(rh.text)
        for wd  in weather[5].find_all("value"):
            WD.append(wd.text)  
        ws = weather[6].find_all("value")
        for k  in range(0,len(ws),2):
            WS.append(ws[k].text)
            BF.append(ws[k+1].text)
        for at  in weather[8].find_all("value"):
            AT.append(at.text)
        wx = weather[9].find_all("value")
        for w in range(0,len(wx),2):
            Wx.append(wx[w].text)
            Wx_n.append(wx[w+1].text)
        rain1 = weather[3].find_all("value")
        len_rain1=len(rain1)
        for l in range(len_rain1):
            pop6 = rain1[l].text
            if len_rain1==11:
                
                if l<len(rain1)-1:
                    PoP6h.append(pop6)
                    PoP6h.append(pop6)
                else:
                    PoP6h.append(pop6)
                    PoP6h.append(pop6)
                    PoP6h.append(pop6)
                    PoP6h.append(pop6)
            elif len_rain1==12:
                PoP6h.append(pop6)
                PoP6h.append(pop6)
                
        rain2 = weather[4].find_all("value")
        for m in range(0,len(rain2)):
            pop12 = rain2[m].text
            PoP12h.append(pop12)
            PoP12h.append(pop12)
            PoP12h.append(pop12)
            PoP12h.append(pop12)


# In[16]:


data = {"CITY":CITY,"DISTRICT":DISTRICT,"GEOCODE":GEOCODE,'DAYTIME':DAYTIME,"DAY" : DAY,"HOUR" : HOUR,"T":T,"TD" : TD,"RH":RH,
        "WD" : WD,"WS" : WS,"BF":BF,"AT" : AT,"Wx": Wx,"Wx_n":Wx_n,"PoP6h" : PoP6h,"PoP12h" :PoP12h,"get_day":get_day}
df = pd.DataFrame(data)


# In[17]:


today_now = str(datetime.datetime.now())[:16].replace('-','').replace(' ','_').replace(':','')
save_name = "taiwan_cwb" + today_now+'_pre' + ".csv"
save_name = file_path + "/" + cwb_data + "/" + save_name
df.to_csv(save_name,index=False,encoding="utf_8_sig")


# In[19]:


listdata=df.values.tolist()


# In[21]:


db = pymysql.connect('140.119.9.88','isvmsdata','dataisvms2018','open_data' ) 
cursor = db.cursor()

insertsql = 'insert into weather_predict(CITY,DISTRICT,GEOCODE,DAYTIME,DAY,HOUR,T,TD,RH,WD,WS,BF,AT,Wx,Wx_n,PoP6h,PoP12h,get_day) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)'
cursor.executemany(insertsql,listdata)
db.commit()
cursor.close()
db.close()

