# crawl_cwb_weather
## 中央氣象局 氣象資料爬蟲

1. 透過連接中央氣象局所提供之API KEY，下載所需之即時觀測與預測資料
   link: https://opendata.cwb.gov.tw/dataset/forecast?page=1
   (1) cwb_pre.py 為即時預測資料，透過XML格式進行爬蟲
   (2) cwb_py.py 為即時觀測資料，透過JSON格式進行爬蟲
   
2.透過連接中央氣象局之互動式網站，進行歷史觀測資料網頁爬蟲
   link:  https://e-service.cwb.gov.tw/HistoryDataQuery/index.jsp
