#!/usr/bin/python
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, MapType, IntegerType, DoubleType, BooleanType,DateType
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
import pandas as pd
import numpy as np
import time
from fake_useragent import UserAgent
import os
import csv
from dotenv import load_dotenv
import requests
import json
from tqdm import tqdm
from pyspark import SparkContext
from pyspark import SparkConf

def webscrape_twitter_atp(output_path) :
    # Retrieve twitter data about ATP Tour
    load_dotenv()  # take environment variable from .env

    # To set your environment variables in your terminal run the following line:
    bearer_token = os.getenv("API_KEY")

    def bearer_oauth(r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def get_rules():
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        return response.json()

    def delete_all_rules(rules):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

    get_rules()
    delete_all_rules(get_rules())
    get_rules()

    # Lets define a function to pass the headers with our bearer_token
    def create_headers(bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def create_headers(bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    # Let's define another function to set the rules to filter the live stream of tweets
    def set_rules(headers, sample_rules):
        # You can adjust the rules if needed
        payload = {"add": sample_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=headers,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )

    # Let's set the code to get the stream of tweets
    def get_stream(output_path, headers, max_number_tweets):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
        )
        # print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        counter_tweets = 0
        stream_tweets = {'stream_tweets': []}

        for response_line in tqdm(response.iter_lines()):
            if response_line:
                json_response = json.loads(response_line)
                stream_tweets["stream_tweets"].append(json_response)

                # print(json.dumps(json_response, indent=4, sort_keys=True))
                counter_tweets += 1

                if counter_tweets >= max_number_tweets:
                    with open(output_path+"atp_tweets_final.json", 'w') as f:
                        json.dump(stream_tweets, f, indent=4)

                    # Close the stream of tweets
                    response.close()

                    # Break the loop
                    break

    start = time.time()

    # Create the headers for the HTTP request
    headers = create_headers(bearer_token)

    # Set the rules for the filtering of tweets that mention the Indian Wells tournament
    # Filtering the time of the tournament and the top 5 players from singles and double each

    sample_rules = [
        {"value": "BNPPARIBASOPEN lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},  # tournament name
        {"value": "IndianWells lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "BNPP022 lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Daniil Medvedev lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},  # Top 5 single player name
        {"value": "Novak Djokovic lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Alexander Zverev lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Rafael Nadal lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Stefanos Tsitsipas lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Mate Pavic lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},  # Top 5 double player name
        {"value": "Nikola Mektic lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Joe Salisbury lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Rajeev Ram lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
        {"value": "Horacio Zeballos lang:en -is:retweet", "activity_start_time": "2022-03-07T00:00:00Z",
         "activity_end_time": "2022-03-20T23:59:59Z"},
    ]

    # @BNPPARIBASOPEN #IndianWells #BNPPO22

    set_rules_add = set_rules(headers, sample_rules)

    print("WEBSCRAPING TWEETS: IN PROGRESS")

    # Get the stream of tweets live
    max_number_tweets = 10
    get_stream(output_path, headers, max_number_tweets)

    print("WEBSCRAPING TWEETS: DONE")

def write_to_parquet(df, filename, output_path) :
    # Export to parquet file
    df.write.mode('overwrite').parquet(output_path+filename)

def read_from_parquet(spark, output_path) :
    # Import from parquet file
    df = spark.read.parquet(output_path+"twitter.parquet")
    df.show()
    return df

def load_twitter_data_to_mongo(output_path, spark, mongo_ip, collection_name) :

    print("LOADING TWITTER DATA TO MONGODB: IN PROGRESS")

    path = output_path+"atp_tweets_final.json"
    with open(path) as json_file:
        data = json.load(json_file)

    data = data["stream_tweets"]

    df = spark.createDataFrame(data=data, schema=["data", "matching_rules"])
    df.printSchema()

    schema = StructType([
        StructField('data', StringType(), True),
    ])

    # Parse the tweet
    df2 = df.rdd.map(lambda x:
                     (x.data["id"], x.data["text"])).toDF(["id", "text"])
    # Retrieve only relevant columns
    df2.show()

    df2.write \
        .format("mongo") \
        .option("spark.mongodb.input.uri",
                mongo_ip+collection_name) \
        .option("spark.mongodb.output.uri",
                mongo_ip+collection_name) \
        .mode("append") \
        .save()

    data_1 = spark.read \
        .format("mongo") \
        .option("spark.mongodb.input.uri",
                mongo_ip+collection_name) \
        .load()

    print("LOADING TWITTER DATA TO MONGODB: DONE")

    print("EXPORTING TWITTER DATA TO PARQUET: IN PROGRESS")

    write_to_parquet(df2, "twitter.parquet", output_path)

    print("EXPORTING TWITTER DATA TO PARQUET: DONE")

def init_browser():
    # Initialize Chrome browser with Fake User Agent
    ua = UserAgent()
    userAgent = ua.chrome

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(f'user-agent={userAgent}')

    browser = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    return browser

def scrape_atp_player_rank(output_path) :
    ### Scrape Player's Ranks

    browser = init_browser()

    # Prepare file
    header = ['player', 'rank', 'age', 'player_point', 'tournament_played', 'next_best']
    with open(output_path+'tennis_ranking_data.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)

    # Open browser
    browser.get('https://www.atptour.com/en/rankings/singles?rankRange=1-5000&rankDate=2022-03-07')
    time.sleep(10)

    # Run selenium to pull data
    with open(output_path+'tennis_ranking_data.csv', 'a', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)

        for i in tqdm(range(1, 2001)):
            player = browser.find_element(by=By.XPATH, value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(
                i) + ']/td[4]/span/a')
            rank = browser.find_element(by=By.XPATH,
                                        value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(i) + ']/td[1]')
            age = browser.find_element(by=By.XPATH,
                                       value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(i) + ']/td[5]')
            point = browser.find_element(by=By.XPATH,
                                         value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(i) + ']/td[6]/a')
            tourn_played = browser.find_element(by=By.XPATH, value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(
                i) + ']/td[7]/a')
            next_best = browser.find_element(by=By.XPATH,
                                             value='//*[@id="player-rank-detail-ajax"]/tbody/tr[' + str(i) + ']/td[9]')

            data = [player.text, rank.text, age.text, point.text, tourn_played.text, next_best.text]
            writer.writerow(data)

    # Store data into dataframe
    df_ranking=pd.read_csv(output_path+'tennis_ranking_data.csv')

    # Assigning value for player's id
    df_ranking["id_player"] = df_ranking.index + 1

    # Drop row with empty value
    df_ranking = df_ranking.dropna(how='any', subset=['age'])

    # Remove decimal character
    df_ranking['player_point'].replace(',', '', regex=True, inplace=True)

    # Creating temporary value for id_player 0
    temp = {'player': 'NaN', 'rank': 'NaN', 'age': 'NaN',
            'player_point': 'NaN', 'tournament_played': 'NaN', 'next_best': 'NaN',
            'id_player': '0', }

    df_ranking = df_ranking.append(temp, ignore_index=True)

    # Change datatype
    df_ranking['id_player'] = df_ranking['id_player'].astype(np.int64)

    browser.close()

    return df_ranking

def scrape_atp_leaderboard(output_path, df_ranking) :
    ### Scrape Leaderboard

    # Prepare file
    header = ['player', 'serve_rating', '1st_serve', '1st_serve_points_won',
              '2nd_serve_points_won', 'service_games_won_percent', 'avg_aces_per_match', 'avg_double_faults_per_match']

    with open(output_path+'tennis_leaderboard_serve.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)

    attempts = 0

    # Multiple attempts to handle webscraping failure
    while attempts < 20:
        try:
            # Open browser
            browser = init_browser()
            browser.get(
                'https://www.atptour.com/en/stats/leaderboard?boardType=serve&timeFrame=Career&surface=all&versusRank=all&formerNo1=false')
            time.sleep(20)

            # Loop through the list

            with open(output_path+'tennis_leaderboard_serve.csv', 'a', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)

                for j in tqdm(range(1, 201)):
                    player = browser.find_element(by=By.XPATH,
                                                  value='//*[@id="leaderboardTable"]/tr[' + str(j) + ']/td[2]/div/a')
                    serve_rating = browser.find_element(by=By.XPATH,
                                                        value='//*[@id="leaderboardTable"]/tr[' + str(j) + ']/td[3]')
                    serve_1st = browser.find_element(by=By.XPATH,
                                                     value='//*[@id="leaderboardTable"]/tr[' + str(j) + ']/td[4]')
                    serve_won_1st = browser.find_element(by=By.XPATH,
                                                         value='//*[@id="leaderboardTable"]/tr[' + str(j) + ']/td[5]')
                    serve_won_2nd = browser.find_element(by=By.XPATH,
                                                         value='//*[@id="leaderboardTable"]/tr[' + str(j) + ']/td[6]')
                    service_games_won = browser.find_element(by=By.XPATH,
                                                             value='//*[@id="leaderboardTable"]/tr[' + str(
                                                                 j) + ']/td[7]')
                    avg_aces_per_match = browser.find_element(by=By.XPATH,
                                                              value='//*[@id="leaderboardTable"]/tr[' + str(
                                                                  j) + ']/td[8]')
                    avg_double_fault_per_match = browser.find_element(by=By.XPATH,
                                                                      value='//*[@id="leaderboardTable"]/tr[' + str(
                                                                          j) + ']/td[9]')

                    data = [player.text, serve_rating.text, serve_1st.text, serve_won_1st.text, serve_won_2nd.text,
                            service_games_won.text, avg_aces_per_match.text, avg_double_fault_per_match.text]
                    writer.writerow(data)
            break
        except NoSuchElementException:
            attempts += 1
            print ("Error while webscraping the page, retrying...")

    # Store data into dataframe
    df_leaderboard_serve=pd.read_csv(output_path+'tennis_leaderboard_serve.csv')

    # Map id_player to leaderboard dataframe
    df_subset = df_ranking[['id_player', 'player']]
    df_leaderboard_mapped = pd.merge(df_leaderboard_serve, df_subset, on='player', how='left')

    # Fill 0 to unmatch values
    df_leaderboard_mapped['id_player'] = df_leaderboard_mapped['id_player'].fillna(0)
    df_leaderboard_mapped['id_player'] = df_leaderboard_mapped['id_player'].astype(np.int64)

    # Assigning value for player's id
    df_leaderboard_mapped["id_leaderboard"] = df_leaderboard_mapped.index + 1

    # Removing unwanted character
    df_leaderboard_mapped['1st_serve'] = df_leaderboard_mapped['1st_serve'].str.replace('%', '')
    df_leaderboard_mapped['1st_serve_points_won'] = df_leaderboard_mapped['1st_serve_points_won'].str.replace('%', '')
    df_leaderboard_mapped['2nd_serve_points_won'] = df_leaderboard_mapped['2nd_serve_points_won'].str.replace('%', '')
    df_leaderboard_mapped['service_games_won_percent'] = df_leaderboard_mapped['service_games_won_percent'].str.replace(
        '%', '')

    df_leaderboard_mapped['1st_serve'] = df_leaderboard_mapped['1st_serve'].astype(np.float64)
    df_leaderboard_mapped['1st_serve_points_won'] = df_leaderboard_mapped['1st_serve_points_won'].astype(np.float64)
    df_leaderboard_mapped['2nd_serve_points_won'] = df_leaderboard_mapped['2nd_serve_points_won'].astype(np.float64)
    df_leaderboard_mapped['service_games_won_percent'] = df_leaderboard_mapped['service_games_won_percent'].astype(
        np.float64)

    browser.close()

    return df_leaderboard_mapped, df_subset

def scrape_atp_tournament_page(output_path):
    ### Scrape Tournament Page

    browser = init_browser()

    # Open browser
    browser.get('https://www.atptour.com/en/scores/results-archive?year=2022')
    time.sleep(10)

    header = ['tournament_name', 'tournament_location', 'tournament_date',
              'tournament_draw', 'tournament_surface', 'tournament_financial_commitment']

    with open(output_path+'tournament_details_scrape.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)

    all_offers = browser.find_elements_by_class_name("tourney-result")

    data = []

    for offer in tqdm(all_offers):
        # uses `offer` instead `driver` to search only in this one offer
        # uses `element` instead of `elements` to search only one value

        try:
            title_content = offer.find_element_by_class_name("title-content").text.strip()
        except Exception as ex:
            title_content = 'NAN'

        tournament_name = offer.find_element_by_class_name("tourney-title").text.strip()
        tournament_location = offer.find_element_by_class_name("tourney-location").text.strip()
        tournament_date = offer.find_element_by_class_name("tourney-dates").text.strip()
        tournament_financial_commitment = offer.find_element_by_class_name("tourney-details.fin-commit").text.strip()

        # looping through the tourney_detail class to get each of tournament draw and surface
        # as these have same class names and we were unable to get values by using class names
        # as done above for other variables.
        all_offers1 = browser.find_elements_by_class_name("tourney-details")
        count = 0
        for offer1 in all_offers1:
            count = count + 1
            if count == 1:
                tournament_draw = offer1.text
            elif count == 2:
                tournament_surface = offer1.text
            else:
                break

        data.append([tournament_name, tournament_location, tournament_date, tournament_draw, tournament_surface,
                     tournament_financial_commitment])

    with open(output_path+'tournament_details_scrape.csv', 'a', encoding='UTF8', newline='') as f:
        # for row in data:
        write = csv.writer(f)
        write.writerows(data)

    df_tournament = pd.read_csv(output_path+'tournament_details_scrape.csv')

    # Assigning value for id_tournament
    df_tournament["id_tournament"] = df_tournament.index + 1

    browser.close()

    return df_tournament

def scrape_atp_match_single(df_subset) :
    ### Scrape Match Single

    browser = init_browser()

    # Open web page
    web_single = 'https://www.atptour.com/en/scores/current/indian-wells/404/results?matchType=singles'

    # Open browser single
    browser.get(web_single)

    time.sleep(10)

    # Define class for match score
    all_data = browser.find_element(by=By.XPATH, value='/html/body/div[3]/div[2]/div[1]/div/div[5]/div/table')
    players = all_data.find_elements(by=By.CLASS_NAME, value='day-table-name')
    scores = all_data.find_elements(by=By.CLASS_NAME, value='day-table-score')

    # Create dataframe for player's details
    match_player = dict()
    match_score = dict()

    match_player_single = pd.DataFrame()
    match_score_single = pd.DataFrame()

    # Fetch players & scores data for single match

    for player in tqdm(players):
        match_player['player'] = player.text
        match_player_single = match_player_single.append(match_player, ignore_index=True)

    for score in tqdm(scores):
        match_score['score'] = score.text
        match_score_single = match_score_single.append(match_score, ignore_index=True)

    # Player 1 and Player 2 data has similar class. Thus, we need to split even & odd row into two different column.
    match_player_single

    # Split players data into player 1 & player 2
    player_1 = match_player_single.iloc[::2]
    player_2 = match_player_single.iloc[1::2]
    player_1 = player_1.reset_index(drop=True)
    player_2 = player_2.reset_index(drop=True)

    # Rename column
    player_1.rename(columns={'player': 'player_1'}, inplace=True)
    player_2.rename(columns={'player': 'player_2'}, inplace=True)

    # Concat all columns
    merge_df = pd.concat([player_1, player_2, match_score_single], axis=1)

    # Rename column
    merge_df.rename(columns={'player': 'player_1'}, inplace=True)

    # Split score into columns
    score_split = merge_df['score'].str.split(' ', expand=True)

    # Merge back score values
    match_single_df = pd.concat([merge_df, score_split], axis=1)
    match_single_df.rename(columns={0: 'set_1', 1: 'set_2', 2: 'set_3'}, inplace=True)

    # Split score into columns
    score_split = merge_df['score'].str.split(' ', expand=True)

    # Merge back score values
    match_single_df = pd.concat([merge_df, score_split], axis=1)
    match_single_df.rename(columns={0: 'set_1', 1: 'set_2', 2: 'set_3'}, inplace=True)

    # Map id_player to match dataframe
    df_match_single_mapped = pd.merge(match_single_df, df_subset, left_on='player_1', right_on='player', how='left')
    df_match_single_mapped.rename(columns={'id_player': 'id_player_1'}, inplace=True)

    df_match_single_mapped = pd.merge(df_match_single_mapped, df_subset, left_on='player_2', right_on='player',
                                      how='left')
    df_match_single_mapped.rename(columns={'id_player': 'id_player_2'}, inplace=True)

    df_match_single_mapped.drop(columns=['player_x', 'player_y'], inplace=True)

    # Change datatype
    df_match_single_mapped['id_player_1'] = df_match_single_mapped['id_player_1'].fillna(0)
    df_match_single_mapped['id_player_2'] = df_match_single_mapped['id_player_2'].fillna(0)

    df_match_single_mapped['id_player_1'] = df_match_single_mapped['id_player_1'].astype(np.int64)
    df_match_single_mapped['id_player_2'] = df_match_single_mapped['id_player_2'].astype(np.int64)

    browser.close()

    return df_match_single_mapped

def scrape_atp_match_double(df_subset, df_match_single_mapped) :
    ### Scrape Match Double

    browser = init_browser()

    # Open web page
    web_double = 'https://www.atptour.com/en/scores/current/indian-wells/404/results?matchType=doubles'

    # Open browser double
    browser.get(web_double)

    # Define class for match
    all_data = browser.find_element(by=By.XPATH, value='/html/body/div[3]/div[2]/div[1]/div/div[5]/div/table')
    players = all_data.find_elements(by=By.CLASS_NAME, value='day-table-name')
    scores = all_data.find_elements(by=By.CLASS_NAME, value='day-table-score')

    # Create dataframe
    match_player = dict()
    match_score = dict()
    match_player_double = pd.DataFrame()
    match_score_double = pd.DataFrame()

    # Fetch players & scores data for single match
    for player in tqdm(players):
        match_player['player'] = player.text
        match_player_double = match_player_double.append(match_player, ignore_index=True)

    for score in tqdm(scores):
        match_score['score'] = score.text
        match_score_double = match_score_double.append(match_score, ignore_index=True)

    # Split player into two columns
    match_player_double = match_player_double['player'].str.split('\n', expand=True)

    # Rename column
    match_player_double.rename(columns={0: 't1_player_1', 1: 't1_player_2'}, inplace=True)

    # Split players data into player 1 & player 2
    t1 = match_player_double.iloc[::2]
    t2 = match_player_double.iloc[1::2]

    t1 = t1.reset_index(drop=True)
    t2 = t2.reset_index(drop=True)

    # Rename column for team 2
    t2.rename(columns={'t1_player_1': 't2_player_1', 't1_player_2': 't2_player_2'}, inplace=True)

    # Concat all columns
    merge_double_df = pd.concat([t1, t2, match_score_double], axis=1)

    # Split score into columns
    score_split = merge_double_df['score'].str.split(' ', expand=True)

    # Merge back score values
    match_double_df = pd.concat([merge_double_df, score_split], axis=1)
    match_double_df.rename(columns={0: 'set_1', 1: 'set_2', 2: 'set_3'}, inplace=True)

    # Map id_player to match dataframe
    df_match_double_mapped = pd.merge(match_double_df, df_subset, left_on='t1_player_1', right_on='player', how='left')
    df_match_double_mapped.rename(columns={'id_player': 'id_player_t1_1'}, inplace=True)

    df_match_double_mapped = pd.merge(df_match_double_mapped, df_subset, left_on='t1_player_2', right_on='player',
                                      how='left')
    df_match_double_mapped.rename(columns={'id_player': 'id_player_t1_2'}, inplace=True)

    df_match_double_mapped = pd.merge(df_match_double_mapped, df_subset, left_on='t2_player_1', right_on='player',
                                      how='left')
    df_match_double_mapped.rename(columns={'id_player': 'id_player_t2_1'}, inplace=True)

    df_match_double_mapped = pd.merge(df_match_double_mapped, df_subset, left_on='t2_player_2', right_on='player',
                                      how='left')
    df_match_double_mapped.rename(columns={'id_player': 'id_player_t2_2'}, inplace=True)

    df_match_double_mapped.drop(columns=['player_x', 'player_y'], inplace=True)

    # Fill NaN data and change datatype
    df_match_double_mapped['id_player_t1_1'] = df_match_double_mapped['id_player_t1_1'].fillna(0)
    df_match_double_mapped['id_player_t1_2'] = df_match_double_mapped['id_player_t1_2'].fillna(0)
    df_match_double_mapped['id_player_t2_1'] = df_match_double_mapped['id_player_t2_1'].fillna(0)
    df_match_double_mapped['id_player_t2_2'] = df_match_double_mapped['id_player_t2_2'].fillna(0)

    df_match_double_mapped['id_player_t1_1'] = df_match_double_mapped['id_player_t1_1'].astype(np.int64)
    df_match_double_mapped['id_player_t1_1'] = df_match_double_mapped['id_player_t1_2'].astype(np.int64)
    df_match_double_mapped['id_player_t2_1'] = df_match_double_mapped['id_player_t2_1'].astype(np.int64)
    df_match_double_mapped['id_player_t2_2'] = df_match_double_mapped['id_player_t2_2'].astype(np.int64)

    # Assign match title & price to main dataset
    df_match_single_mapped = df_match_single_mapped.assign(id_tournament='19')
    df_match_double_mapped = df_match_double_mapped.assign(id_tournament='19')

    # Change datatype
    df_match_single_mapped['id_player_1'] = df_match_single_mapped['id_player_1'].astype(np.int64)
    df_match_single_mapped['id_tournament'] = df_match_single_mapped['id_tournament'].astype(np.int64)
    df_match_double_mapped['id_tournament'] = df_match_double_mapped['id_tournament'].astype(np.int64)
    df_match_double_mapped['id_player_t1_2'] = df_match_double_mapped['id_player_t1_2'].astype(np.int64)

    # Assigning value for id_match
    df_match_single_mapped["id_match"] = df_match_single_mapped.index + 1
    df_match_double_mapped["id_match"] = df_match_double_mapped.index + 1

    browser.close()

    return df_match_double_mapped, df_match_single_mapped

def export_to_csv(df, filename, isUsingSeparator, decimal):
    # Export dataframe to CSV
    if isUsingSeparator:
        df.to_csv(filename, sep=';', index=False, decimal=decimal)
    else:
        df.to_csv(filename, index=False, decimal=decimal)


def webscrape_atp_data(output_path) :
    # Webscrape from various pages on ATP Tours website
    print("WEBSCRAPING ATP TOURS: IN PROGRESS")

    df_ranking = scrape_atp_player_rank(output_path)

    df_leaderboard_mapped, df_subset = scrape_atp_leaderboard(output_path, df_ranking)

    df_tournament = scrape_atp_tournament_page(output_path)

    df_match_single_mapped = scrape_atp_match_single(df_subset)

    df_match_double_mapped, df_match_single_mapped = scrape_atp_match_double(df_subset, df_match_single_mapped)

    # Write to csv file
    export_to_csv(df_ranking, output_path+"atp_rank_scrapped.csv", False, ",")
    export_to_csv(df_leaderboard_mapped, output_path+"atp_leaderboard_scrapped.csv", False, ".")
    export_to_csv(df_tournament, output_path+"atp_tournament_scrapped.csv", False, ",")
    export_to_csv(df_match_single_mapped, output_path+"match_single_scrapped.csv", True, ",")
    export_to_csv(df_match_double_mapped, output_path+"match_double_scrapped.csv", True, ",")

    print("WEBSCRAPING ATP TOURS: DONE")

def webscrape_bet365(output_path) :
    # Webscrape Bet365 website

    print("WEBSCRAPING BET365: IN PROGRESS")

    attempts = 0
    # Multiple attempts to handle webscraping failure
    while attempts < 20:

        # New Methods
        match_dict = dict()
        odd1_dict = dict()
        odd2_dict = dict()
        match_df = pd.DataFrame()
        odd1_df = pd.DataFrame()
        odd2_df = pd.DataFrame()

        try:
            # Open browser
            browser = init_browser()
            browser.get('https://www.bet365.com/#/AC/B13/C1/D50/E2/F163/')
            time.sleep(20)

            all_data = browser.find_element(by=By.XPATH, value='//div[@class="cm-CouponModule "]')
            all_competitions = all_data.find_elements(by=By.CLASS_NAME, value='src-CompetitionMarketGroup')

            for competition in tqdm(all_competitions):
                competition_name = competition.find_element(by=By.CLASS_NAME,
                                                            value="rcl-CompetitionMarketGroupButton_Title")
                sections = competition.find_elements(by=By.CLASS_NAME, value="gl-Market_General")

                count = 0
                for section in sections:
                    if sections.index(section) == 0:
                        children = section.find_elements(by=By.XPATH, value="./child::*")
                        for child in children:
                            next_children = child.find_elements(by=By.XPATH, value="./child::*")
                            if (len(next_children) == 0):
                                match_date = child.text
                            else:
                                sched = child.find_element(by=By.CLASS_NAME,
                                                           value="rcl-ParticipantFixtureDetails_Details")
                                team_names = child.find_element(by=By.CLASS_NAME,
                                                                value="rcl-ParticipantFixtureDetails_TeamNames")
                                ## Add new data
                                match_dict['competition_name'] = competition_name.text
                                match_dict['match_date'] = match_date
                                match_dict['sched'] = sched.text
                                match_dict['teams'] = team_names.text
                                match_df = pd.concat([match_df, pd.DataFrame(match_dict, index=[0])],
                                                     ignore_index=True)
                    elif sections.index(section) == 1:
                        odds_1 = section.find_elements(by=By.CLASS_NAME, value="gl-Market_General-cn1")
                        for odd_1 in odds_1:
                            odd1_dict['1'] = odd_1.text
                            odd1_df = pd.concat([odd1_df, pd.DataFrame(odd1_dict, index=[0])],
                                                ignore_index=True)
                    elif sections.index(section) == 2:
                        odds_2 = section.find_elements(by=By.CLASS_NAME, value="gl-Market_General-cn1")
                        for odd_2 in odds_2:
                            # Record timestamp
                            odd2_dict['2'] = odd_2.text
                            currentTime = str(datetime.now())
                            odd2_dict['timestamp'] = currentTime
                            odd2_df = pd.concat([odd2_df, pd.DataFrame(odd2_dict, index=[0])],
                                                ignore_index=True)
            break
        except NoSuchElementException:
            attempts += 1
            print ("Error while webscraping the page, retrying...")


    # Merge the dataframes
    df_tennis365 = pd.concat([match_df, odd1_df, odd2_df], axis=1)

    output_file = output_path+'tennis365.csv'
    df_tennis365.to_csv(output_file, mode='a', header=not os.path.exists(output_path))

    print("WEBSCRAPING BET365: DONE")

    browser.close()

def connect_mongodb() :
    # Initialize PySpark - MongoDB connection/session
    working_directory = 'jars/*'
    mongo_ip = "mongodb://mongo_docker:27017/twitter."

    spark = SparkSession.builder.appName('PySparkMongo') \
        .config('spark.driver.extraClassPath', working_directory) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.master","local") \
        .getOrCreate()

    collection_name = 'twitter_data'

    return spark, mongo_ip, collection_name


def connect_postgres() :
    # Initialize PySpark - Postgres connection/session
    conf = SparkConf()
    conf.setMaster("local").setAppName("PySparkPostgres")
    sc = SparkContext(conf=conf)

    jdbc_url = 'jdbc:postgresql://postgres/postgres'
    connection_properties = {
        'user': 'postgres',
        'password': 'postgres',
        'driver': 'org.postgresql.Driver',
        'stringtype': 'unspecified'}

    session = pyspark.sql.SparkSession(sc)

    return session, connection_properties, jdbc_url


def load_atptours_data_to_postgres(output_path, session, connection_properties, jdbc_url) :

    print("LOADING ATP TOURS DATA TO POSTGRES: IN PROGRESS")

    # Rank
    df_rank = session.read.option("header", True) \
        .option("delimiter", ",") \
        .csv(output_path+"atp_rank_scrapped.csv")
    df_rank = df_rank.withColumn("id_player", df_rank['id_player'].cast(IntegerType()))

    # Tournament
    df_tournament = session.read.format("csv").option("header", True) \
        .option("delimiter", ",") \
        .load(output_path+"atp_tournament_scrapped.csv")
    df_tournament = df_tournament.withColumn("id_tournament", df_tournament['id_tournament'].cast(IntegerType()))
    df_tournament.printSchema()
    df_tournament.show()

    # Leaderboard
    df_leaderboard = session.read.format("csv").option("header", True) \
        .option("delimiter", ",") \
        .load(output_path+"atp_leaderboard_scrapped.csv")
    df_leaderboard = df_leaderboard.withColumn("id_player", df_leaderboard['id_player'].cast(IntegerType()))
    df_leaderboard = df_leaderboard.withColumn("id_leaderboard", df_leaderboard['id_leaderboard'].cast(IntegerType()))
    df_leaderboard = df_leaderboard.withColumn("serve_rating", df_leaderboard['serve_rating'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("1st_serve", df_leaderboard['1st_serve'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("1st_serve_points_won",
                                               df_leaderboard['1st_serve_points_won'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("2nd_serve_points_won",
                                               df_leaderboard['2nd_serve_points_won'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("service_games_won_percent",
                                               df_leaderboard['service_games_won_percent'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("avg_aces_per_match",
                                               df_leaderboard['avg_aces_per_match'].cast(DoubleType()))
    df_leaderboard = df_leaderboard.withColumn("avg_double_faults_per_match",
                                               df_leaderboard['avg_double_faults_per_match'].cast(DoubleType()))
    df_leaderboard.printSchema()
    df_leaderboard.show()

    # Match Single
    df_match_single = session.read.format("csv").option("header", True) \
        .option("delimiter", ";") \
        .load(output_path+"match_single_scrapped.csv")
    df_match_single = df_match_single.withColumn("id_match", df_match_single['id_match'].cast(IntegerType()))
    df_match_single = df_match_single.withColumn("id_tournament", df_match_single['id_tournament'].cast(IntegerType()))
    df_match_single = df_match_single.withColumn("id_player_1", df_match_single['id_player_1'].cast(IntegerType()))
    df_match_single = df_match_single.withColumn("id_player_2", df_match_single['id_player_2'].cast(IntegerType()))
    df_match_single.printSchema()
    df_match_single.show()

    # Match Double
    df_match_double = session.read.format("csv").option("header", True) \
        .option("delimiter", ";") \
        .load(output_path+"match_double_scrapped.csv")
    df_match_double = df_match_double.withColumn("id_match", df_match_double['id_match'].cast(IntegerType()))
    df_match_double = df_match_double.withColumn("id_tournament", df_match_double['id_tournament'].cast(IntegerType()))
    df_match_double = df_match_double.withColumn("id_player_t1_1",
                                                 df_match_double['id_player_t1_1'].cast(IntegerType()))
    df_match_double = df_match_double.withColumn("id_player_t1_2",
                                                 df_match_double['id_player_t1_2'].cast(IntegerType()))
    df_match_double = df_match_double.withColumn("id_player_t2_1",
                                                 df_match_double['id_player_t2_1'].cast(IntegerType()))
    df_match_double = df_match_double.withColumn("id_player_t2_2",
                                                 df_match_double['id_player_t2_2'].cast(IntegerType()))
    df_match_double.printSchema()
    df_match_double.show()

    # Write data to database
    df_rank.select('player', 'rank', 'age', 'player_point', 'tournament_played', 'next_best', 'id_player') \
            .write.jdbc(jdbc_url, "atp_tour_schema.atp_rank", mode="append", properties=connection_properties)

    df_tournament.select('tournament_name', 'tournament_location', 'tournament_date', 'tournament_draw',
                         'tournament_surface', 'tournament_financial_commitment', 'id_tournament') \
            .write.jdbc(jdbc_url, "atp_tour_schema.tournament", mode="append", properties=connection_properties)

    df_leaderboard.select('id_leaderboard', 'id_player', 'player', 'serve_rating', '1st_serve',
                          '1st_serve_points_won', '2nd_serve_points_won', 'service_games_won_percent',
                          'avg_aces_per_match', 'avg_double_faults_per_match') \
            .write.jdbc(jdbc_url, 'atp_tour_schema.atp_leaderboard', mode="append", properties=connection_properties)

    df_match_single.select('player_1', 'player_2', 'score', 'set_1', 'set_2', 'set_3', 'id_player_1', 'id_player_2',
                           'id_tournament', 'id_match') \
            .write.jdbc(jdbc_url, "atp_tour_schema.atp_indianwells_single", mode="append", properties=connection_properties)

    df_match_double.select('id_match', 'id_tournament', 'id_player_t1_1', 'id_player_t1_2', 'id_player_t2_1',
                           'id_player_t2_2',
                           't1_player_1', 't1_player_2', "t2_player_1", "t2_player_2", 'score', 'set_1', 'set_2',
                           'set_3') \
            .write.jdbc(jdbc_url, "atp_tour_schema.atp_indianwells_double", mode="append", properties=connection_properties)

    print("LOADING ATP TOURS DATA TO POSTGRES: DONE")

    # Export data to parquet file
    print("EXPORTING ATP TOURS DATA TO PARQUET: IN PROGRESS")

    write_to_parquet(df_rank, "atp_rank.parquet",output_path)
    write_to_parquet(df_tournament, "tournament.parquet", output_path)
    write_to_parquet(df_leaderboard, "atp_leaderboard.parquet", output_path)
    write_to_parquet(df_match_single, "atp_indianwells_single.parquet", output_path)
    write_to_parquet(df_match_double, "atp_indianwells_double.parquet", output_path)

    print("EXPORTING ATP TOURS DATA TO PARQUET: DONE")


if __name__ == '__main__':

    # Specify path for output files
    output_path = 'output_files/'
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    try:
        # PySpark MongoDB connection
        pyspark_mongo_session, mongo_ip, collection_name = connect_mongodb()

        # Get Twitter data
        webscrape_twitter_atp(output_path)

        # Load twitter data to Mongo
        load_twitter_data_to_mongo(output_path, pyspark_mongo_session, mongo_ip, collection_name)

        # Close session
        pyspark_mongo_session.stop()

        # PySpark Postgres connection
        pyspark_postgres_session, connection_properties, jdbc_url = connect_postgres()

        # Get data from bet365 website
        webscrape_bet365(output_path)

        # Get data from ATP Website and export to CSV
        webscrape_atp_data(output_path)

        # Load data from CSV to Postgres
        load_atptours_data_to_postgres(output_path, pyspark_postgres_session, connection_properties, jdbc_url)

        # Close session
        pyspark_postgres_session.stop()

    finally:
        print("END OF PROCESS")