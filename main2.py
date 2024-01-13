import requests
import mysql.connector
import csv
from mysql.connector import Error
from datetime import datetime
import re
import time

API_KEY = "" # Insert your google API key right here
BASE_URL = "https://www.googleapis.com/youtube/v3/"

try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='1234'
    )
    db_Info = connection.get_server_info()
    cursor = connection.cursor()

    # Check if database exists, if not create one
    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]
    if 'creators' not in databases:
        cursor.execute("CREATE DATABASE creators")
        print("Database 'creators' created successfully!")

    # Now, use the newly created database
    cursor.execute("USE creators")

except Error as e:
    print(e)


def extract_social_links(description):
    # Enhanced regex to match URLs with and without 'http' or 'https'.
    # This regex captures urls starting directly with domain name (like twitch.tv/username) and those starting with http(s)://
    urls = re.findall(r'(?:(?:http[s]?://)?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)', description)

    social_links = {
        'twitter': None,
        'twitch': None,
        'instagram': None,
        'tiktok': None,
        'facebook': None
    }

    for url in urls:
        if "twitter.com" in url:
            social_links['twitter'] = url
        elif "twitch.tv" in url:
            social_links['twitch'] = url
        elif "instagram.com" in url:
            social_links['instagram'] = url
        elif "tiktok.com" in url:
            social_links['tiktok'] = url
        elif "facebook.com" in url:
            social_links['facebook'] = url

    return social_links

def get_category_id(api_key, category_name):
    url = BASE_URL + "videoCategories"
    params = {"part": "snippet", "regionCode": "US", "key": api_key}
    response = requests.get(url, params=params)
    data = response.json()
    for item in data.get("items", []):
        if item["snippet"]["title"].lower() == category_name.lower():
            return item["id"]
    return None

def get_top_channels_in_category(api_key, gaming_category_id, max_results=1000):
    url = BASE_URL + "videos"
    params = {"part": "snippet", "chart": "mostPopular", "videoCategoryId": gaming_category_id, "maxResults": 50, "key": api_key}
    channel_ids = set()
    total_requests = max_results // 50
    for _ in range(2):
        response = requests.get(url, params=params)
        data = response.json()
        for item in data.get("items", []):
            channel_ids.add(item['snippet']['channelId'])
        if len(channel_ids) >= 1000:
            break
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
        params["pageToken"] = next_page_token

    channels = []
    url = BASE_URL + "channels"
    params = {"part": "snippet,statistics,brandingSettings", "key": api_key}
    for index, channel_id in enumerate(channel_ids):
        if index >= 1000:
            break
        params['id'] = channel_id
        response = requests.get(url, params=params)
        data = response.json()
        channels.extend(data.get("items", []))
    return channels

def store_to_db(channels):
    cursor = connection.cursor()
    try:
        # Creating the 'creators' table if it doesn't exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS creators (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            channel_id VARCHAR(500) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            subscribers BIGINT NOT NULL,
            account_created DATE)''')

        # Check if 'creator_social_links' table exists
        cursor.execute("SHOW TABLES LIKE 'creator_social_links'")
        if cursor.fetchone():
            # If table exists, then we attempt to add UNIQUE constraint (if not already added)
            try:
                cursor.execute("ALTER TABLE creator_social_links ADD UNIQUE (creator_id)")
            except Error as e:
                # Expected error if UNIQUE constraint already exists; you can suppress this or log it if you want
                pass
        else:
            # If table doesn't exist, create it
            cursor.execute('''
                CREATE TABLE creator_social_links (
                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                    creator_id INTEGER UNIQUE,
                    discord VARCHAR(500),
                    twitch VARCHAR(500),
                    twitter VARCHAR(500),
                    patreon VARCHAR(500),
                    facebook VARCHAR(500),
                    instagram VARCHAR(500),
                    tiktok VARCHAR(500),
                    other TEXT,
                    FOREIGN KEY (creator_id) REFERENCES creators(id)
                )
            ''')

        for channel in channels:
            cursor.execute('''
                        INSERT INTO creators (channel_id, name, subscribers, account_created)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name), subscribers=VALUES(subscribers), account_created=VALUES(account_created)
                    ''', (channel['id'], channel['snippet']['title'], channel['statistics']['subscriberCount'],
                          channel['snippet']['publishedAt'].split("T")[0]))

            # Check if a new row was inserted or an existing row was updated
            if cursor.rowcount == 1:  # A new row was inserted
                creator_id = cursor.lastrowid
            else:  # An existing row was updated
                cursor.execute('SELECT id FROM creators WHERE channel_id = %s', (channel['id'],))
                creator_id = cursor.fetchone()[0]

            # Extracting the links
            social_links = extract_social_links(channel['snippet']['description'])

            cursor.execute('''
                    INSERT INTO creator_social_links (creator_id, discord, twitch, twitter, instagram, tiktok, facebook)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE discord=VALUES(discord), twitch=VALUES(twitch), twitter=VALUES(twitter), instagram=VALUES(instagram), tiktok=VALUES(tiktok), facebook=VALUES(facebook)
                ''', (creator_id, social_links.get('discord', None), social_links.get('twitch', None), social_links.get('twitter', None), social_links.get('instagram', None), social_links.get('tiktok', None), social_links.get('facebook', None)))

        connection.commit()

    except Error as e:
        print("Error while storing to DB", e)
    finally:
        cursor.close()
        connection.close()



def save_to_csv(channels):
    with open('youtube_gaming_creators.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = ['ID', 'Creator_Name', 'Subscribers']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for index, channel in enumerate(channels, 1):
            writer.writerow({'ID': index, 'Creator_Name': channel['snippet']['title'], 'Subscribers': channel['statistics']['subscriberCount']})

if __name__ == "__main__":
    gaming_category_id = get_category_id(API_KEY, "Gaming")
    if gaming_category_id:
        top_gaming_creators = get_top_channels_in_category(API_KEY, gaming_category_id)
        store_to_db(top_gaming_creators)
        save_to_csv(top_gaming_creators)
    connection.close()
