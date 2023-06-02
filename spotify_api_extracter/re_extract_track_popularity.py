import base64
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pprint
import requests
import json
import csv
import datetime
import time
import logging
import os
import sys
import errno
import queue
import threading

# 기본적인 정보값 설정
current_directory_path = '/' + \
    '/'.join(os.path.realpath(__file__).split('/')[:-1])
now = datetime.datetime.now()
ymd = str(now.year)+str(now.month).zfill(2)+str(now.day).zfill(2)
timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')


def get_access_token(client_id, client_secret):
    """
    Spotify API를 사용할 때 필요한 Token 인증 방식
    - 1시간동안 유효한 토큰 발급
    """
    auth_header = base64.b64encode("{}:{}".format(client_id, client_secret).encode(
        'utf-8')).decode('ascii')    # Base64로 인코딩된 인증 헤더 생성
    token_url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f'Basic {auth_header}'
    }
    payload = {
        "grant_type": "client_credentials"
    }

    response = requests.post(token_url, data=payload, headers=headers)
    access_token = json.loads(response.text)["access_token"]

    return {"Authorization": f"Bearer {access_token}"}


def make_log(ymd):
    """
    Logging 라이브러리를 사용하여, 로그를 통해 complete OR error 모니터링
    """
    mylogger = logging.getLogger(ymd)
    mylogger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream_hander = logging.StreamHandler()
    stream_hander.setFormatter(formatter)
    mylogger.addHandler(stream_hander)

    try:
        if not (os.path.isdir(current_directory_path + "/log/")):
            os.makedirs(os.path.join(current_directory_path + "/log/"))
    except OSError as e:
        if e.errno != errno.EEXIST:
            print("Failed to create directory!!!!!")
            raise

    file_handler = logging.FileHandler(
        current_directory_path + "/log/"+ymd+".log")
    mylogger.addHandler(file_handler)

    return mylogger


def add_lists_to_csv(file_path, lists):
    """
    만들어둔 csv 파일에 list 추가
    """
    with open(file_path, 'a', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        for list in lists:
            csvwriter.writerow(list)


def run_thread(start, end, data_q, error_q, token_queue):
    cnt = 0
    access_token = token_queue.get()

    thread_track_list = []
    for idx, line in enumerate(total_track_list[start:end]):
        track_key = line[0]                 # artist_id

        retries = 0
        max_retries = 5
        wait_time = 0

        while retries < max_retries:
            # TRACK 호출
            track_url = f"https://api.spotify.com/v1/tracks/{track_key}"
            r = requests.get(track_url, headers=access_token)

            if r.status_code == 200:

                track = r.json()
                track_popularity = track["popularity"]
                if track_popularity is None:
                    print(track_key)
                    time.sleep(5)
                track_result = line + [track.get("popularity", None)]
                thread_track_list.append(track_result)
                mylogger.info(
                    f"TRACK DONE [{idx+1}/{len(total_track_list[start:end])}] || track_id :: {track_key} track_popularity :: {track_popularity}")
                break

            elif r.status_code == 429:
                # API 제한이 발생
                # exceed API 이슈 존재하여, retires와 header에 있는 wait 정보값을 이용하여 해결
                wait_time = int(r.headers.get('Retry-After', 0))
                mylogger.info(
                    f"TRACK ISSUE {track_key} || 429 status_code / API exceed issue")
                mylogger.info(
                    f"Rate limited. Waiting for {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1

            else:
                mylogger.info(f"TRACK ERROR || artist_id :: {track_key} ")
                mylogger.info(
                    f"TRACK ERROR || album_id :: {track_key} Artist: status_code : {r.status_code} error_msg : {r.text}")

        data_q.put(1)
    cnt+1
    add_lists_to_csv(new_artist_album_track_path, thread_track_list)
    # Thread를 기준으로 artist가 가지고 있는 앨범 저장
    mylogger.info(
        f"THRED WRITE [{cnt+1}] ALBUM on kpop_artist_album_data.csv")
    token_queue.put(access_token)


if __name__ == "__main__":

    # secret json 가져오기 - extract와 동일한 경로에 secret 업로드
    with open('./secret.json', 'r') as jsonfile:
        client_info = json.load(jsonfile)

    # main token
    CLIENT_ID = client_info["client_id"]
    CLIENT_SECRET = client_info["client_secret"]

    # 대표 TOKEN
    # Artist 추출할 때 처음 지정할 token 생성
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    print(access_token)
    sys.exit()

    # TOKEN queue 만들기
    token_queue = queue.Queue()
    for client_id, client_secret in client_info["client_info"]:
        token_queue.put(get_access_token(client_id, client_secret))

    # FILE LOCATION 고정 변수
    DATA_PATH = './result/' + ymd + '/'
    ERROR_PATH = './errors/' + ymd + '/'

    artist_album_track_path = DATA_PATH + \
        'kpop_artist_album_track_data_v2.csv'    # track csv 저장 경로
    new_artist_album_track_path = DATA_PATH + 'kpop_artist_album_track_data_v4.csv'
    os.makedirs(DATA_PATH, exist_ok=True)

    # Logger
    mylogger = make_log(timestamp)

    #####################################################
    #               ARTIST 추출
    #####################################################

    # 만약 스크래핑을 이미 해왔다면, csv를 기준으로 항목 가져오기
    total_track_list = []
    with open(artist_album_track_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)
        for row in csvreader:
            total_track_list.append(row)
    time.sleep(1)
    # print(len(total_track_list)) # 8745

    # total_track_list = total_track_list[:10]
    # print(total_track_list)

    # KPOP ARTIST ALBUM의 TRACK 정보 - csv 파일 우선 생성
    with open(new_artist_album_track_path, 'w', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['id', 'name', 'track_href', 'external_url', 'artist_id', 'artist_name', 'album_id', 'track_number', 'acousticness', 'analysis_url', 'danceability',
                           'duration_ms', 'energy',  'instrumentalness', 'liveness', 'loudness', 'mode', 'speechiness', 'tempo', 'time_signature', 'valence', 'popularity'])

    #################################################
    #                   Thread
    #################################################
    data_q = queue.Queue()
    error_q = queue.Queue()

    thread_count = 20
    thread_list = []

    work = len(total_track_list) // thread_count

    for i in range(thread_count):
        start = i * work

        if i == thread_count - 1:
            end = len(total_track_list)
        else:
            end = (i+1) * work
        thread_list.append(threading.Thread(target=run_thread, args=(
            start, end, data_q, error_q, token_queue)))

    [thread.start() for thread in thread_list]
    [thread.join() for thread in thread_list]

    # ERROR 확인
    data_count = data_q.qsize()
    error_count = error_q.qsize()

    if error_count != 0:
        os.makedirs(ERROR_PATH, exist_ok=True)
        with open(ERROR_PATH + 'error_artist_data.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['artist_key', 'err_msg', 'err_lineno'])

            while not error_q.empty():
                csvwriter.writerow(error_q.get())

    # CNT 확인 - ARTIST 기준 추출
    print('=====================================')
    print(f"data_count :: {data_count}")
    print(f"error_count :: {error_count}")
    print('=====================================')
    mylogger.info(f"data_count :: {data_count}")
    mylogger.info(f"error_count :: {error_count}")
    sys.exit(0)
    #########################################################
    # S3에 업로드
    # - 다음 명령어는 s3 cli 설치해서 aws configure을 등록해주어야 함
    # - 작성자는 access key 만듬
    ##########################################################
    os.system(
        f'aws s3 cp {artist_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(
        f"upload S3 bucket {artist_path} to s3://spotify-kpop-analysis/result_data/")
    os.system(
        f'aws s3 cp {artist_album_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(
        f"upload S3 bucket {artist_album_path} to s3 s3://spotify-kpop-analysis/result_data/")
    os.system(
        f'aws s3 cp {artist_album_track_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(
        f"upload S3 bucket {artist_album_track_path} s3://spotify-kpop-analysis/result_data/")
    mylogger.info(f"DONE")
