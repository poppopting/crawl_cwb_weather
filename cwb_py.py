
# coding: utf-8

# In[1]:


import urllib.request
import os
import json
import pandas as pd
import datetime
import numpy as np
import pymysql
pd.options.display.max_columns = None
pd.options.display.max_rows = 300
pd.options.display.max_info_rows = None


# In[2]:


#file_path=r'C:\isvms'
file_path = r'D:\open_data\weather_reality'
cwb_data = "cwb_weather_data"
if not os.path.exists(file_path+'\\'+cwb_data):
    os.mkdir(file_path+'\\'+cwb_data)


# In[3]:


url='https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={formatt}'
dataid='O-A0001-001'
apikey='YOUR APIKEY'
formatt='json'
link=url.format(dataid=dataid,apikey=apikey,formatt=formatt)
#link
data_path=file_path+'\\'+cwb_data+'\\'+dataid+'.'+formatt
data_path


# In[4]:


url='https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={formatt}'
dataid='O-A0001-001'
apikey='CWB-D931EFB8-3C85-4845-869E-665B13359FC6'
formatt='json'
link=url.format(dataid=dataid,apikey=apikey,formatt=formatt)
#link
data_path=file_path+'\\'+cwb_data+'\\'+dataid+'.'+formatt

def auto_down(url,filename):
    try:
        urllib.request.urlretrieve(url,filename)
    except :
        print('Download error!!! Reloading now.')
        auto_down(url,filename)


auto_down(link,data_path)


# In[5]:


with open(data_path,'r',encoding='utf-8-sig') as file:
    api=json.load(file)
access_time=api['cwbopendata']['sent'][:-6].replace('-','/').replace('T',' ')


# In[6]:


district_weathers=api['cwbopendata']['location']


# In[7]:


lat=[]  #float
lon=[]  #float
lat_wgs84=[] #float
lon_wgs84=[] #float
locationName=[] #string
stationId=[]  #string
observe_time=[] #date
DAY=[]   #data
HOUR=[]  #float
ELEV=[]   #float
WDIR=[]   #float
WDSD=[]   #float
TEMP=[]   #float
HUMD=[]   #float
PRES=[]   #float
SUN=[]    #float
H_24R=[]  #float
H_FX=[]   #float
H_XD=[]   #float
H_FXT=[]  #date
D_TX=[]   #float
D_TXT=[]   #date
D_TN=[]   #float
D_TNT=[]   #date
CITY=[]   #string
CITY_SN=[]  #float
TOWN=[]    #string
TOWN_SN=[]  #float
for dist_weather in district_weathers:
    lat.append(float(dist_weather['lat']))
    lon.append(float(dist_weather['lon']))
    lat_wgs84.append(float(dist_weather['lat_wgs84']))
    lon_wgs84.append(float(dist_weather['lon_wgs84']))
    locationName.append(dist_weather['locationName'])
    stationId.append(dist_weather['stationId'])
    time=dist_weather['time']['obsTime'][:-6].replace('-','/').replace('T',' ')
    observe_time.append(time)
    day,time=time.split(' ')
    DAY.append(day)
    HOUR.append(time[:2])
    ELEV.append(float(dist_weather['weatherElement'][0]['elementValue']['value']))
    WDIR.append(float(dist_weather['weatherElement'][1]['elementValue']['value']))
    WDSD.append(float(dist_weather['weatherElement'][2]['elementValue']['value']))
    TEMP.append(float(dist_weather['weatherElement'][3]['elementValue']['value']))
    HUMD.append(float(dist_weather['weatherElement'][4]['elementValue']['value']))
    PRES.append(float(dist_weather['weatherElement'][5]['elementValue']['value']))
    SUN.append(float(dist_weather['weatherElement'][6]['elementValue']['value']))
    H_24R.append(float(dist_weather['weatherElement'][7]['elementValue']['value']))
    H_FX.append(float(dist_weather['weatherElement'][8]['elementValue']['value']))
    H_XD.append(float(dist_weather['weatherElement'][9]['elementValue']['value']))
    H_FXT.append(dist_weather['weatherElement'][10]['elementValue']['value'][:-6].replace('-','/').replace('T',' '))
    D_TX.append(float(dist_weather['weatherElement'][11]['elementValue']['value']))
    D_TXT.append(dist_weather['weatherElement'][12]['elementValue']['value'][:-6].replace('-','/').replace('T',' '))
    D_TN.append(float(dist_weather['weatherElement'][13]['elementValue']['value']))
    D_TNT.append(dist_weather['weatherElement'][14]['elementValue']['value'][:-6].replace('-','/').replace('T',' '))
    CITY.append(dist_weather['parameter'][0]['parameterValue'])
    CITY_SN.append(float(dist_weather['parameter'][1]['parameterValue']))
    TOWN.append(dist_weather['parameter'][2]['parameterValue'])
    TOWN_SN.append(float(dist_weather['parameter'][3]['parameterValue']))
get_day=[access_time]*len(lat)


# In[8]:


cwb=pd.DataFrame({'observe_time':observe_time,'DAY':DAY,'HOUR':HOUR,'CITY':CITY,'TOWN':TOWN,'locationName':locationName,'CITY_SN':CITY_SN,
                  'TOWN_SN':TOWN_SN,'stationId':stationId,'lat':lat,'lon':lon,'lat_wgs84':lat_wgs84,'lon_wgs84':lon_wgs84,
                  'ELEV':ELEV,'WDIR':WDIR,'WDSD':WDSD,'TEMP':TEMP,'HUMD':HUMD,'PRES':PRES,'SUN':SUN,'H_24R':H_24R,'H_FX':H_FX,
                  'H_XD':H_XD,'H_FXT':H_FXT,'D_TX':D_TX,'D_TXT':D_TXT,'D_TN':D_TN,'D_TNT':D_TNT,'access_time':access_time})
cwb=cwb.replace(-99.0,np.NaN)
cwb.head()


# In[16]:


cwb=cwb.astype(object).where(pd.notnull(cwb), None)


# In[17]:


access=access_time[:-3].replace('/','').replace(' ','_').replace(':','')
today = str(datetime.date.today())
save_name = "taiwan_cwb" + access+'_obs' + ".csv"
save_name = file_path + "/" + cwb_data + "/" + save_name
cwb.to_csv(save_name,index=False,encoding="utf_8_sig")


# In[18]:


listdata=cwb.values.tolist()


# In[19]:


db = pymysql.connect('IP','ACCOUNT','PASSWORD','DATABASE' ) 
cursor = db.cursor()

insertsql = 'insert into weather_reality(observe_time, DAY, HOUR, CITY, TOWN,locationName,CITY_SN,TOWN_SN,stationId,lat,lon,lat_wgs84,lon_wgs84,ELEV,WDIR,WDSD,TEMP,HUMD,PRES,SUN,H_24R,H_FX, H_XD, H_FXT, D_TX, D_TXT, D_TN, D_TNT,access_time) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)'
cursor.executemany(insertsql,listdata)
db.commit()
cursor.close()
db.close()

