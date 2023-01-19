from sqlalchemy import create_engine
from datetime import date, datetime ,timedelta
import configparser
import pandas as pd
import requests
import os
import time
import json
from PIL import Image, ImageFilter
from io import BytesIO
from hashlib import sha256
import sys
import re
class DBconnector:

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config_flickr.ini")
        self.engine = create_engine(f"mysql+pymysql://{self.config['MySQL']['user']}:"
                                    f"{self.config['MySQL']['passw']}@{self.config['MySQL']['host']}:"
                                    f"{self.config['MySQL']['port']}/{self.config['MySQL']['db']}")



class FlickrImageDownload:
    def __init__(self, keyword=None):
        self.config = configparser.ConfigParser()
        self.config.read("config_flickr.ini")
        self.config_path = self.config['Download']['path']
        self.api_key = self.config['FLICKR']['id']
        self.license= self.config['Download']['license']
        self.checkInit =True
        self.config_format =self.config['Process']['image_format']
        if not (os.path.exists(self.config_path)):
            print("path not exists")
            self.checkInit = False
        if keyword:
            self.keyword =keyword
        else:
            self.keyword = self.config['Download']['search']

    def reset_counts(self):
        self.download_count = 0
        self.start_time = time.time()
        self.last_update = 0
        self.download_count = 0
        self.error_count = 0
        self.cached = 0
        self.sources = []
        self.urls=[]

    def create_request_get_urls(self , method, query):
        try:
            response_pic = requests.get('https://www.flickr.com/services/rest/?method=flickr.photos.search',
                                        params=query)
            response_pic.raise_for_status()
            if response_pic.status_code != 200:
                print('status code returned', response_pic.text['massage'])
                raise Exception
            if not json.loads(response_pic.text)['photos']['photo']:
                print('error no photos')
                raise Exception
            photos = json.loads(response_pic.text)['photos']['photo']
            urls = []
            for photo in photos:
                url = photo.get(self.license)
                if url:
                    # print(url)
                    urls.append(url)
            return urls
        except requests.exceptions.HTTPError as errh:
            print(errh)
        except requests.exceptions.ConnectionError as errc:
            print(errc)
        except requests.exceptions.Timeout as errt:
            print(errt)
        except requests.exceptions.RequestException as err:
            print(err)
        except Exception as e:
            print(e)
        return None
    def load_image(self, url):
        try:
            response = requests.get(url)
            h = sha256(response.content).hexdigest()
            img = Image.open(BytesIO(response.content))
            img.load()
            return img, h
        except KeyboardInterrupt:
            sys.exit(0)
        except:
            return None, None

    def check_to_keep_photo(self, url,image,keywords, dir_path):
        h = sha256(image.tobytes()).hexdigest()
        p = os.path.join(dir_path, f"{keywords}-{h}.{self.config_format}")
        self.sources.append([url,p,h,keywords])
        if not os.path.exists(p):
            self.download_count += 1
            return p
        else:
            self.cached += 1
            return None

    def obtain_photo(self, url):
        if url:
            image, h = self.load_image(url)
            if image:
                return image
        self.error_count += 1
        return None


    def write_sources(self,tableName='images'):
            dframe = pd.DataFrame(self.sources, columns=['url','file_loc','hashed_sha256','keyword'])
            dframe = dframe.astype({"url": 'string', "file_loc":'string' ,"hashed_sha256":'string' ,"keyword":'string'})
            try:
                print(dframe.head())
                c1 = DBconnector()
                dbConnection = c1.engine.connect()
                frame = dframe.to_sql(tableName, dbConnection, if_exists='append', index=False,chunksize=10
                                     )
            except ValueError as vx:
                print("valueError" ,vx)
            except Exception as ex:
                print("except" ,ex)
            else:
                print("Table %s created successfully." % tableName)
            finally:
                dbConnection.close()







def main():
    scrape_Flickr()
    print(search_key_scraped('glass',size=200).head())


def search_key_scraped(keyword , min_date=None , max_date=None , size =100):
    if not(min_date ) or not(max_date):
        min_date = (date.today() - timedelta(days=30)).isoformat()
        max_date = (date.today() + timedelta(days=1)).isoformat()
    try:
        c1 = DBconnector()
        dbConnection= c1.engine.connect()
        query = f"select * from images where scrape_datetime between '{min_date}' and '{max_date}'  and keyword='{keyword}' limit {size}"
        dataf = pd.read_sql(query, dbConnection)
        return dataf
    except Exception as e:
        print(e)
        return None

def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return f"{h}:{m:>02}:{s:>05.2f}"

def create_dir(keywords, config_path):
    directory = re.sub('[^\w]','_',keywords)
    dir_path = os.path.join(config_path, directory)
    if not (os.path.exists(dir_path)):
        os.mkdir(dir_path)
    return dir_path




def scrape_Flickr(keywords=None, max_per_page=100):
        flk = FlickrImageDownload(keywords)
        flk.reset_counts()
        if not flk.checkInit:
            print('No Path Exists')
            raise Exception
        query = {'api_key': flk.api_key,'format': 'json','text': flk.keyword,'tags': flk.keyword,'tag_mode': 'all','extras': 'url_c,license','sort': 'relevance','nojsoncallback': '1', 'per_page': max_per_page}
        method= 'flickr.photos.search'
        flk.urls = flk.create_request_get_urls(method ,query)
        if flk.urls:
            dir_path = create_dir(flk.keyword, flk.config_path)
            for url in flk.urls:
                img = flk.obtain_photo(url)
                if img:
                    sub_keywords=  re.sub('[^\w]','_',flk.keyword)
                    path = flk.check_to_keep_photo(url,img,sub_keywords,dir_path)
                    if path:
                        img.save(path)
            flk.write_sources()
            elapsed_time = time.time() - flk.start_time
            print("Complete, elapsed time: {}".format(hms_string(elapsed_time)))

if __name__ == "__main__":
    main()
