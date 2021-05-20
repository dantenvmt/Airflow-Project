from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
import praw
import pandas as pd
import os
import datetime
import requests
import nltk
nltk.downloader.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import argparse
import os
import csv

# set up Reddit API
reddit = praw.Reddit(client_id='o5HoS-P9uefXsQ', client_secret='9rBM2ILgyRne7tiu3xJjPrZqIXJvRA', user_agent='dantenvmt')
wsb = reddit.subreddit('wallstreetbets')

# common WSB terminology and associated sentiments
wsb_words = {
    'tendies': 2.0,
    'bull': 2.0,
    'bear': -2.0,
    'puts': -2.0,
    'calls': 2.0,
    'hold': 2.0,
    'moon': 2.0,
    'btfd': 2.0,
    'going tits up': -2.0,
    'diamond': 2.0,
    'yolo':2.0,
    'bullish':2.0,
    'free':2.0,
    'rocket':2.0,
    'gain':2.0,
    'buy':2.0,
    'discount':2.0,
    'tendie':2.0,
    'elon':2.0,
    '420':2.0,
    'sell':-2.0,
    'put':-2.0,
    'short':-2.0,
    'shorts':-2.0,
    'bagholder':-2.0,
    'paper':-2.0,
    'shorting':-2.0,
    'citron':-2.0,
    'hedge':-2.0,
    'retard':-2.0,
    'retards': 0.0,
    'retarded': 0.0,
    'fucking': 0.0,
    'fuck': 0.0,
    'autist': 0.0,
    'gay': 0.0,
    'stonk': 2.0,




}

def get_sentiment(df_col, words=wsb_words):

    # initializes VADER
    vader = SentimentIntensityAnalyzer()

    # updates VADER's lexicon to include WSB terminology
    vader.lexicon.update(wsb_words)

    # analyzes sentiment of dataframe column input
    scores = df_col.apply(vader.polarity_scores).tolist()
    return pd.DataFrame(scores)

def get_date(submission):
    
    # gets submission datetime
    try:
        time = submission.created
    except: 
        time = submission
    return datetime.datetime.fromtimestamp(time)

def scrape_for(symbol, limit=10, after='120d', subreddit='wallstreetbets'):
    result = []
    url_for_comment = 'https://api.pushshift.io/reddit/search/comment/?q={0}&subreddit={1}&after={2}&size={3}&sort_type=score'.format(symbol, subreddit, after, limit)
    comments = requests.get(url_for_comment).json()['data']

    url_for_submission = 'https://api.pushshift.io/reddit/search/submission/?q={0}&subreddit={1}&after={2}&size={3}&sort_type=score'.format(symbol, subreddit, after, limit)
    submissions = requests.get(url_for_submission).json()['data']

    for comment in comments:
        result.append((comment['body'], get_date(comment['created_utc'])))

    for submission in submissions:
        date = get_date(submission['created_utc'])
        try:
            result.append((submission['title'] + submission['selftext'], date))
        except:
            result.append((submission['title'], date))
        sub = reddit.submission(id=submission['id'])
        sub.comments.replace_more(limit=0)
        for top_level_comment in sub.comments:
            result.append((top_level_comment.body, date))

    # creates a dataframe from API results
    df = pd.DataFrame(result, columns=['Text', 'Date'])

    # updates dataframe to include sentiments
    scored_df = df.join(get_sentiment(df['Text']), rsuffix='_right')

    return scored_df

def main():

    results = scrape_for('GME',30,'1d','wallstreetbets')
    results.to_csv('../a_sentiment.csv')
    print(results)

default_args = {
	'owner': 'default_user',
	'start_date': airflow.utils.dates.days_ago(1),
	'depends_on_past': False,
	  #With this set to true, the pipeline won't run if the previous day failed
	'email': ['dantenvmt@gmail.com'],
	'email_on_failure': True,
	 #upon failure this pipeline will send an email to your email set above
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
}
dag = DAG(
	'UwU',
	default_args=default_args,
	#schedule_interval='00 20 * * *',
)

export = PythonOperator(dag=dag,task_id='export', python_callable=main)
