FROM apache/airflow:2.0.2
RUN pip install --no-cache-dir praw
RUN pip install --no-cache-dir requests
RUN pip install --no-cache-dir nltk
RUN pip install --no-cache-dir argparse