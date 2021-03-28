#! /usr/bin/python3

# Script that downloads data about recently rented apartments at the room.nl website

import json
import requests
from bs4 import BeautifulSoup
import pymysql
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

conn = pymysql.connect(
        host=config["DATABASE"]["HOST"],
        user=config["DATABASE"]["USER"],
        password=config["DATABASE"]["PASSWD"],
        db=config["DATABASE"]["DB"])

# Get newest data from room.nl/en/recently-rented
def update_data():
    # Get raw html of website, find the table of recently rented apartments
    # and put items in table to list
    html_raw = requests.get("https://www.room.nl/en/recently-rented/").text
    soup = BeautifulSoup(html_raw, 'html.parser')
    table = soup.find(id='justification-table').tbody
    rooms = table.find_all('tr')

    # Connect to database
    with conn:
        with conn.cursor() as cursor:
            for r in rooms:
                # Find all apartments from Leiden and insert into database ignoring duplicates
                data = r.find_all('td')
                if data[1].string == 'Leiden':  
                    sql = "INSERT IGNORE INTO rooms (address, type, reactions, date, allocation)\
                            VALUES (%s, %s, %s, %s, %s);"
                    cursor.execute(sql, 
                            (data[0].text, data[2].text, data[3].text, data[4].text, data[5].text))
        conn.commit()

# Return all data from rooms table in database
def get_data():
    with conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
           sql = "SELECT * FROM rooms;"
           cursor.execute(sql)
           return(cursor.fetchall())
