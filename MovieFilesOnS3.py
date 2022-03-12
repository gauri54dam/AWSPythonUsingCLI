from logging import exception
from types import NoneType
import boto3
from bs4 import BeautifulSoup as bs
import requests
import json
from datetime import datetime
import pandas as pd

# Step 1 :- Input Data - Connect to webpage using requests library and scrape the webpage data using BeautifulSoup
# Step 2 :- Data Selection - Print the sample HTML page to select the data for further processing processing
# Step 3 :- Data Cleaning  - convert date into coorect data type, movie running time should be integert value etc.
# Step 4 :- Load the json into dataframe for analytics

## Comments - Web scrapping is messier than fetching data from API endoint!
## Web scrapping can have multiple edge cases which needs to be solved to get clean data


def clean_tag(sp):
    for tag in sp.find_all(['sup','span']):
        tag.decompose()


def min_toInt(running_time):
    if running_time == 'N/A':
        return None
    if isinstance(running_time, list):
        return int(running_time[0].split(" ")[0])
    else:
        return int(running_time.split(" ")[0])

def date_conversion(dt):

    if isinstance(dt, list):
        dt =  dt[0]

    if dt == 'N/A':
        return None

    dt = dt.split("(")[0].strip()
    print(dt)
    fmt= "%B %d, %Y"

    try :
        return datetime.strptime(dt, fmt)
    except:
        pass
  


def content_value(row_data):
    if row_data.find("li"):
        return [li.get_text(" ", strip =True).replace("\xa0", " ") for li in row_data.find_all("li")]
    elif row_data.find("br"):
        return [text for text in row_data.stripped_strings]
    else:
        return row_data.get_text(" ", strip =True).replace("\xa0", " ")

def get_info_box(url):
    r = requests.get(url)
    soup = bs(r.content)
    info_box = soup.find(class_="infobox vevent")
    clean_tag(soup)
    rows_info_box = info_box.find_all("tr")
    movie = {}
    for index, row in enumerate(rows_info_box):
        if index == 0:
            movie['title'] = row.find("th").get_text(" ", strip =True)
        else:
            header = row.find('th')
            if header:
                key = row.find("th").get_text(" ", strip =True)
                value = content_value(row.find("td"))
                movie[key] = value
    return movie


r = requests.get("https://en.wikipedia.org/wiki/List_of_Walt_Disney_Pictures_films")
soup = bs(r.content)
movies = soup.select(".wikitable.sortable i a")
base_path = "https://en.wikipedia.org/"

movie_list=[]
for index, movie in enumerate(movies):

    try :
        path = movie['href']
        full_path = base_path+path
        title = movie['title']
        #print(path)
        #print(title)
        #print()
        movie_list.append(get_info_box(full_path))
    except Exception as e:
        movie.get_text()
        print(e)
        break

## int data type convertion
for movie in movie_list:
    movie['Running time int'] = min_toInt(movie.get('Running time', 'N/A'))
## datetime type conversion

for movie in movie_list:
    movie['Release date (date)'] = date_conversion(movie.get('Release date', 'N/A'))

## change the date format to make it report ready


df = pd.DataFrame(movie_list)
df = df.drop("Running time", axis =1)
df = df.drop("Release date", axis =1)
df = df.drop("Release dates", axis =1)
df.to_csv('movies.csv', index=False)

s3 = boto3.client('s3')
s3.upload_file("movies.csv","mytestsgithub","movies.csv")