#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
import time
import numpy as np
import re
import pandas as pd
from pymongo import MongoClient
import json
import time
import random
import glob


# In[5]:



headers = {'user-agent': 'Chrome/99.0.4840.0'}
url = "https://www.yelp.com/search?find_desc=donut+shop&find_loc=San+Francisco%2C+CA&start="

pages = np.arange(0, 31, 10)
pgn = 1
soup_pages = {}
for page in pages:
    url2 = url+str(page)
    response = requests.get(url2, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'lxml')
    soup_pages[pgn] = soup    
    with open('sf_donut_shop_search_page_'+str(pgn)+'.html', 'w+',encoding="utf-8") as file:
        file.write(str(soup))
    pgn = pgn+1
    time.sleep(12)


# In[6]:


def yelp_database(item):
        if item.find('h3'): 
            
            name = item.find('h3').text.strip().replace(".","")
            search_dict = dict()
            if re.match('[1234567890]+',name):
                search_dict['Rank'] = re.findall('\d+', name)[0]
                search_dict['Name'] = item.find('a', {'class': 'css-1422juy'}).get_text()
                search_dict['Url'] = "https://www.yelp.com"+item.find('a',href=True)['href']
                search_dict['Number of Reviews'] = item.find_all('span', {'class': 'reviewCount__09f24__tnBk4 css-1e4fdj9'})[0].text.strip()
                search_dict['Star Rating'] = item.select('[aria-label*=rating]')[0]['aria-label']



                dollar = item.find_all('span', {'class': 'priceRange__09f24__mmOuH css-18qxe2r'})
                if len(dollar) > 0:
                    search_dict['Dollar Sign'] = dollar[0].text.strip()
                else:
                    search_dict['Dollar Sign'] = None
                
                
                
                
                tags = item.find_all('p', {'class': 'css-1p8aobs'})
                if len(tags) > 0:
                    search_dict['Tags'] = [t.text.strip() for t in tags]
                else:
                    search_dict['Tags'] = None



                if item.select('[class*=css-1enow5j]'):
                    search_dict['Yelp Order'] = True
                else:
                    search_dict['Yelp Order'] = False



                if item.select('[class*=tag__]'):
                    feature = dict()

                    for tags in item.select('[class*=tag__]'):
                        if tags.select('[class*=icon--16-close-v2]'):
                            tag1 = False
                        else:
                            tag1 = True
                        label = tags.find_all('p', {'class': 'tagText__09f24__ArEfy iaTagText__09f24__Gv1CO css-12bvu5l'})
                        if len(label)>0:
                            label = str(label[0].text.strip())
                            feature[label] = tag1
                    search_dict['Dine in tags'] = feature
                else:
                    search_dict['Dine in tags'] = None
                    
                    
                    
                if search_dict is not None:
                    return search_dict
                else:
                    return None     



# In[11]:



client = MongoClient("mongodb://localhost:27017/")
database = client["Yelp_Database"]
donutsf = database["donut_shops"]

a = 1
for b in range(1,5):
    # read search result pages
    with open("sf_donut_shop_search_page_" + str(b) + ".html", encoding="utf8") as file:
        contents = file.read()
        soup = BeautifulSoup(contents, 'html.parser')
        
        for item in soup.select('[class*=container]'):
            if yelp_database(item) is not None:
                print(yelp_database(item))
                donutsf.insert_one(yelp_database(item))
                b=a+1


# In[12]:



database = client['Yelp_Database']
donutsf = database['donut_shops']


SF_donuts = pd.DataFrame(pd.json_normalize(donutsf.find()))
num = len(SF_donuts)


for i in range(0, num):        
    r = requests.get(SF_donuts['Url'][i], headers = headers)
    time.sleep(random.randint(2,5))
    soup = BeautifulSoup(r.text, 'lxml')
    filename = 'sf_donut_shop_'+str(i)+'.htm'
    with open(filename, 'w+', encoding="utf-8") as fd:
        fd.write(str(soup))


# In[16]:


add = ""

number = ""

urls = []


num = len(SF_donuts)

index = np.arange(0,10,1)

for i in range(0,num):
    with open("sf_donut_shop_" + str(i) + ".htm", encoding="utf8") as file:
        r = file.read()
    soup = BeautifulSoup(r, 'lxml')

    for j in soup.findAll("section",{"class":"margin-b3__09f24__l9v5d border-color--default__09f24__NPAKY"}):
        
        for k in j.findAll("p",{"data-font-weight":"semibold"}):
            
            if(k.text[0]=="("):
                num = k.text
                print("Phone - ",num)
            
            if(k.text[0] in str(index)):
                address = k.text
                print("Address - ",address)

        if(len(j.text)>5):
            idx_ = j.text.startswith('Business website')
            
            if(idx_ == True):
                for website in j.find_all('a',attrs={'href': re.compile("^/biz_redir")},limit=1):
                    urls.append("https://www.yelp.com" + website.get('href'))
                    print('URL - ', "https://www.yelp.com" + website.get('href'))
            
            else:
                urls.append('NA')
                print('URL - NA')
                
    
    if number == "":
        number = num
    
    else:
        number = number + "," + num
    
    
    if add == "":
        add = address
    
    else:
        add = add + ":" + address

Phone = number.split(",")

Address = add.split(":") 


# In[14]:


geoloc = []

num = len(SF_donuts)

for i in range(0, num):
    
    api = requests.request("GET","http://api.positionstack.com/v1/forward?access_key=1d2f64fee8805963f28a0845a341d437&query="+str(Address[i]), headers=headers)
    
    data = json.loads(api.text)
    
    latitude = data['data'][0]['latitude']
    longitude = data['data'][0]['longitude']
    
    geoloc.append("(" + str(latitude) + "," + str(longitude) + ")")


# In[15]:


final =  pd.DataFrame({'Address': Address, 'Phone': Phone, 'Shop_website': website, 'Geoloc': geoloc})

index = len(final)+1

for i in range(1, index):
    value = {"Rank" : str(i)}
    value_2 = {"$set" : final.to_dict(orient = 'records')[i-1] }
    donutsf.update_one(value,value_2)

donutsf.create_index('Rank', name = "Rank_index")

