import base64
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pprint
import requests
import json, csv
import datetime, time
import logging
import os, sys
import errno
import queue
import threading

# 기본적인 정보값 설정
current_directory_path = '/' + '/'.join(os.path.realpath(__file__).split('/')[:-1])
now = datetime.datetime.now()
ymd = str(now.year)+str(now.month).zfill(2)+str(now.day).zfill(2)
timestamp = now.strftime('%Y-%m-%d_%H:%M:%S')

def get_access_token(client_id, client_secret) :
    """
    Spotify API를 사용할 때 필요한 Token 인증 방식
    - 1시간동안 유효한 토큰 발급
    """
    auth_header = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')    # Base64로 인코딩된 인증 헤더 생성
    token_url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization" : f'Basic {auth_header}'
    }
    payload = {
        "grant_type" : "client_credentials"
    }

    response = requests.post(token_url, data = payload, headers= headers)
    access_token = json.loads(response.text)["access_token"]
    
    return {"Authorization" : f"Bearer {access_token}"}

def make_log(ymd):
    """
    Logging 라이브러리를 사용하여, 로그를 통해 complete OR error 모니터링
    """
    mylogger = logging.getLogger(ymd)
    mylogger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream_hander = logging.StreamHandler()
    stream_hander.setFormatter(formatter)
    mylogger.addHandler(stream_hander)

    try:
        if not(os.path.isdir(current_directory_path + "/log/")):
            os.makedirs(os.path.join(current_directory_path + "/log/"))
    except OSError as e:
        if e.errno != errno.EEXIST:
            print("Failed to create directory!!!!!")
            raise



    file_handler = logging.FileHandler(current_directory_path + "/log/"+ymd+".log")
    mylogger.addHandler(file_handler)

    return mylogger

def add_lists_to_csv(file_path, lists) :
    """
    만들어둔 csv 파일에 list 추가
    """
    with open(file_path, 'a', encoding = 'utf-8') as csvfile :
        csvwriter = csv.writer(csvfile)
        for list in lists:
            csvwriter.writerow(list)

def scraping_kpop_artist() : 
    """
    전체 KPOP에 대해 검색했을 때 결과 추출
    - REST API : Spotify search API
    """
    total_artist_list = []
    
    url = "https://api.spotify.com/v1/search"

    # 생성할 csv 파일
    f= open(DATA_PATH + '/kpop_artist_data.csv', 'w')
    wr = csv.writer(f)
    wr.writerow(["id", "name", "genre", "external_url", "image_url", "polularity", "followers"])
    
    # 초기화
    offset = 0
    limit = 50
    cnt = 0
    
    k_genre = ["k-pop", "k-pop girl group", "k-pop boy group", "k-rap", "korean r&b", "korean pop", "korean ost", "k-rap", "korean city pop","classic k-pop", "korean singer-songwriter"]

    while True: 
        
        params = {
            "q" : "genre:K-pop",
            "type" : "artist",
            "offset" : offset, 
            "limit": limit          # 페이지 당 아티스트 수 
        }
        
        response = requests.get(url, headers = access_token, params = params)
        data = response.json()

        if response.status_code == 200 :
            # 정상 응답
            total_artist = data["artists"]["total"] # API에서 retrun 해주는 전체 list 수
            artists = data["artists"]["items"]
            
            for idx, artist in enumerate(artists) :
                artist_id, artist_name = artist["id"], artist["name"]
                

                # search k-pop 조회 결과, 1000개가 조회되는데, k-pop에 해당하지 않는 artist 존재
                if len(list(set(artist["genres"]).intersection(k_genre)))==0:
                    continue
                # 추출 대상
                result_artist = [
                    artist["id"]
                    , artist["name"]
                    , artist["genres"]
                    , artist["external_urls"]["spotify"]
                    , artist["images"][0]["url"] if len(artist["images"]) != 0 else None
                    , artist["popularity"]
                    , artist["followers"]["total"]
                ]
                
                wr.writerow(result_artist)
                total_artist_list.append(artist["id"]) # 다음 함수에서 이어 사용하기 위해
                mylogger.info(f"ARTIST FUCN [{offset + (idx+1)}/{total_artist}] || artist_id :: {artist_id} artist_name :: {artist_name}")
                cnt += 1
                    
            offset += limit
            if offset >= total_artist:
                mylogger.info("ARTIST FUCN [DONE] || 전체 Aritist 결과를 Scraping 해왔습니다.")
                break
                
        else:
            # response error
            mylogger.info(f"ARTIST FUCN [ERROR] || Artist: status_code : {response.status_code} error_msg : {response.text}")
            break
        
    f.close()
    mylogger.info(f"ARTIST FUNC [SUCCESS] || scraping_kpop_artist 실행 완료 - 최종 추출 수량 :: {cnt}")
    
    return total_artist_list



def artist_albums_track(access_token, album_key) :
    """
    ALBUM에 해당하는 track 정보 추출
    """
    albums_track_list, albums_track_issue_list = [], []
    url = f"https://api.spotify.com/v1/albums/{album_key}/tracks"
    
    # 초기화
    retries = 0
    max_retries = 5
    wait_time = 0
    
    offset = 0
    limit = 50
    
    try :
        while retries < max_retries :
            
            params = {
                "offset" : offset,
                "limit" : limit
            }
            
            # album에 해당하는 track 호출
            response = requests.get(url, headers = access_token, params = params)
            data = response.json()
            
            if response.status_code == 200 :
                
                total_track = data["total"]
                
                # album 내에 존재하는 전체 트랙 순회
                for idx, track in enumerate(data["items"]):
                    
                    track_id, track_name = track["id"], track["name"]
                    artist_id, artist_name = track["artists"][0]["id"], track["artists"][0]["name"]
                    
                    # AUDIO_FEATURE 호출 - Track : feature
                    audio_url = f'https://api.spotify.com/v1/audio-features/{track_id}'
                    r = requests.get(audio_url, headers=access_token)
                    feature = r.json()
                    # change_feature : NULL 값이 많이 존재 : 없을 시 None 대체
                    def change_feature(feature):
                        # 노가다 숨기기
                        feature["acousticness"] = feature.get("acousticness", None)
                        feature["analysis_url"] = feature.get("analysis_url", None)
                        feature["danceability"] = feature.get("danceability", None)
                        feature["duration_ms"] = feature.get("duration_ms", None)
                        feature["energy"] = feature.get("energy", None)
                        feature["feature_id"] = feature.get("id", None)
                        feature["instrumentalness"] = feature.get("instrumentalness", None)
                        feature["liveness"] = feature.get("liveness", None)
                        feature["loudness"] = feature.get("loudness", None)
                        feature["mode"] = feature.get("mode", None)
                        feature["speechiness"] = feature.get("speechiness", None)
                        feature["tempo"] = feature.get("tempo", None)
                        feature["time_signature"] = feature.get("time_signature", None)
                        feature["valence"] = feature.get("valence", None)
                        feature["track_href"] = feature.get("track_href", None)
                        return feature
                    feature = change_feature(feature)
                    # track 추출 대상
                    track_result = [
                                track["id"]
                                ,track["name"]
                                ,feature["track_href"]
                                ,track["external_urls"]["spotify"]
                                ,artist_id
                                ,artist_name
                                ,album_key
                                ,track["track_number"]
                                
                                # feature
                                ,feature["acousticness"]
                                ,feature["analysis_url"]
                                ,feature["danceability"]
                                ,feature["duration_ms"]
                                ,feature["energy"]
                                ,feature["instrumentalness"]
                                ,feature["liveness"]
                                ,feature["loudness"]
                                ,feature["mode"]
                                ,feature["speechiness"]
                                ,feature["tempo"]
                                ,feature["time_signature"]
                                ,feature["valence"]
                    ]
                    
                    albums_track_list.append(track_result) # thread 함수에서 csv에 저장할 list 만들기
                    mylogger.info(f"TRACK DONE [{idx+1}/{total_track}] || artist_id :: {artist_id} artist_name :: {artist_name} track_id :: {track_id} track_name :: {track_name} album_id :: {album_key} ")
                    
                offset += limit
                if offset >= total_track : 
                    mylogger.info(f"ARTIST's ALBUM TRACK SUCCESS || {album_key}에 해당하는 전체 Track 탐색이 완료되었습니다.")
                    break
                
            elif response.status_code == 429 :
                # API 제한이 발생
                # exceed API 이슈 존재하여, retires와 header에 있는 wait 정보값을 이용하여 해결
                wait_time = int(response.headers.get('Retry-After', 0))
                mylogger.info(f"TRACK ISSUE {album_key} || 429 status_code / API exceed issue")
                mylogger.info(f"Rate limited. Waiting for {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
                
            else:
                # 그래도 다른 이슈가 존재한다면, log에서 check
                mylogger.info(f"TRACK ERROR || artist_id :: {artist_id} artist_name :: {artist_name} track_id :: {track_id} track_name :: {track_name} album_id :: {album_key}")
                mylogger.info(f"TRACK ERROR || album_id :: {album_key} Artist: status_code : {response.status_code} error_msg : {response.text}")
                albums_track_issue_list.append([album_key, artist_id, artist_name, track_id, track_name])
                break

    except Exception as e:
        # pipe 끊어질 수 있음
        mylogger.info(f"TRACT ERROR || album_id : {album_key} Artist: status_code : {response.status_code} error_msg : {response.text}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        err_lineno = exc_tb.tb_lineno
        
        albums_track_issue_list.append([album_key, e, err_lineno])
        
    return albums_track_list, albums_track_issue_list



def run_thread(start, end, data_q, error_q, token_queue) : 
    cnt = 0
    
    offset = 0
    limit = 50
    
    thread_album_list = []
    
    # ARTIST LIST를 기준으로 SLICING
    for idx, line in enumerate(total_artist_list[start:end]) :
        artist_key = line                  # artist_id
        access_token = token_queue.get()   # token 가져오기
        
        try :
            album_url = f"https://api.spotify.com/v1/artists/{artist_key}/albums"
            
            params = {
                "offset" : offset,
                "limit" : limit
            }
            
            response = requests.get(album_url, headers = access_token, params = params)
            data = response.json()
        
            if response.status_code == 200 :
                
                total_album = data["total"]
                
                # ARTIST의 앨범 순회
                for idx, album in enumerate(data["items"]):
                    
                    try : 
                        album_artist_id = album["artists"][0]["id"]
                        album_id, album_name = album["id"], album["name"]
                        
                        # 일부 데이터에서 artist_id에 없는 값들이 들어가 있었던 것을 확인
                        if album_artist_id != artist_key :
                            continue
                        
                        ###################################################
                        #        해당하는 앨범에 속해있는 TRACK 가져오기
                        ###################################################
                        albums_track_list, albums_track_issue_list = artist_albums_track(access_token, album_id)        # album에 있는 track 스크래핑
                        add_lists_to_csv(artist_album_track_path, albums_track_list)                                    # album에 해당하는 track csv에 추가하기
                        
                        if len(albums_track_issue_list) != 0:
                            # 만약 return 받은 album_track_issue_list가 존재한다면, error csv 로 관리
                            os.makedirs(ERROR_PATH , exist_ok=True)                                                     # ERROR 있으면 생성
                            option = 'a' if os.path.exists(ERROR_PATH + f'/error_album_track_{timestamp}.csv') else 'w' # 최초 생성이면 write / 최초 생성이 아니면 add
                            f = open(ERROR_PATH + f'/error_album_track_{timestamp}.csv', option, encoding = 'utf-8')
                            wr = csv.writer(f)
                            for error_line in albums_track_issue_list : 
                                wr.writerow(error_line)
                            f.close()
                            
                        mylogger.info(f"ALBUM'S TRACK SAVE || artist_id :: {artist_key} album_id :: {album_id}")
                        ###################################################
                        
                        # API에서 추출할 Album 값
                        album_result = [ 
                            album["id"]
                            , album["name"]
                            , album["external_urls"]["spotify"] 
                            , album["artists"][0]["id"]
                            , album["artists"][0]["name"]               # artist가 한명은 아닌 것 같지만..! -> csv 명세에 맞춰 1명으로 고정
                            , album["images"][0]["url"]
                            , album["release_date"]
                            , album["total_tracks"]
                        ]
                        
                        thread_album_list.append(album_result)                  # artist_album을 한 스레드 단위로 추가
                        mylogger.info(f"ALBUM DONE [{idx+1}/{total_album}] || artist_id :: {artist_key} album_id :: {album_id} album_name :: {album_name}")
                    except :
                        mylogger.info(f"ALBUM ERROR [{idx+1}/{total_album}] || artist_id :: {artist_key} album_id :: {album_id} album_name :: {album_name}")
                        
                        
            # 추출한 특정 artist key에 해당하는 artist의 1개의 앨범 완료
            mylogger.info(f"ARTIST's ALBUM SUCCESS [{cnt+1}/{len(total_artist_list[start:end])}] || artist_id :: {artist_key} - artist가 보유한 앨범 추출 완료")
            data_q.put(1)
            cnt += 1
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            err_lineno = exc_tb.tb_lineno
            
            mylogger.info(f"ARTIST's ALBUM ERROR [{cnt+1}/{len(total_artist_list[start:end])}] || artist_id :: {artist_key} error_msg :: {e} err_lieno :: {err_lineno} - artist가 보유한 앨범 추출 실패")
            error_q.put([artist_key, e, err_lineno])
            cnt += 1
        
        # token 반환
        token_queue.put(access_token)
        
    # Thread를 기준으로 artist가 가지고 있는 앨범 저장
    add_lists_to_csv(artist_album_path, thread_album_list)
    mylogger.info(f"THRED WRITE {artist_key} ALBUM on kpop_artist_album_data.csv")
    mylogger.info(f"THREAD DONE || 완료된 artist_key : {artist_key} 완료된 aritst count :: {cnt}")

def extract_track(token_queue) :
    total_track_list = []
    # csv load
    # with open('./load/global_popular_track_id_list.csv', 'r') as csvfile:
    #     csvreader = csv.reader(csvfile)
    #     header = next(csvreader)
    #     for row in csvreader:
    #         total_track_list.append(row[0])
    
    f= open(DATA_PATH + '/global_popular_track.csv', 'w')
    wr = csv.writer(f)
    wr.writerow(['id' , 'name', 'track_href','external_url' , 'artist_id', 'artist_name', 'album_id', 'track_number', 'acousticness', 'analysis_url', 'danceability', 'duration_ms', 'energy',  'instrumentalness', 'liveness', 'loudness', 'mode', 'speechiness', 'tempo', 'time_signature', 'valence'])
    access_token = token_queue.get()
    track_json = dict()
    for idx, track_key in enumerate(total_track_list) :
            
        # TRACK 호출
        track_url = f"https://api.spotify.com/v1/tracks/{track_key}"
        r = requests.get(track_url, headers=access_token)
        track = r.json()
        
        track_json[track_key] = {
            "track_id" : track_key,
            "track_name" : track["name"], 
            "track_external_urls" : track["external_urls"]["spotify"],
            "artist_name" : track["artists"][0]["name"],
            "artist_id" : track["artists"][0]["id"],
            "album_id" : track["album"]["id"],
            "track_number" : track["track_number"]            
        }        
        mylogger.info(f"[{idx+1}/{len(total_track_list)}] - track_id :: {track_key}")
    mylogger.info(f"TRACK INFO 스캔 완료")
    

    token_queue = queue.Queue()
    for client_id, client_secret in client_info :
        token_queue.put(get_access_token(client_id, client_secret))
    
    for idx, track_key in enumerate(total_track_list) :
        access_token = token_queue.get()
        # AUDIO_FEATURE 호출 - Track : feature
        audio_url = f'https://api.spotify.com/v1/audio-features/{track_key}'
        res = requests.get(audio_url, headers=access_token)
        feature = res.json()
        if res.status_code != 200 :
            print(res.text)
            time.sleep(5)
        # change_feature : NULL 값이 많이 존재 : 없을 시 None 대체
        def change_feature(feature):
            # 노가다 숨기기
            feature["acousticness"] = feature.get("acousticness", None)
            feature["analysis_url"] = feature.get("analysis_url", None)
            feature["danceability"] = feature.get("danceability", None)
            feature["duration_ms"] = feature.get("duration_ms", None)
            feature["energy"] = feature.get("energy", None)
            feature["feature_id"] = feature.get("id", None)
            feature["instrumentalness"] = feature.get("instrumentalness", None)
            feature["liveness"] = feature.get("liveness", None)
            feature["loudness"] = feature.get("loudness", None)
            feature["mode"] = feature.get("mode", None)
            feature["speechiness"] = feature.get("speechiness", None)
            feature["tempo"] = feature.get("tempo", None)
            feature["time_signature"] = feature.get("time_signature", None)
            feature["valence"] = feature.get("valence", None)
            feature["track_href"] = feature.get("track_href", None)
            return feature
        feature = change_feature(feature)

        track_result = [
                    track_key
                    ,track_json[track_key]["track_id"]
                    ,feature["track_href"]
                    ,track_json[track_key]["track_external_urls"]
                    ,track_json[track_key]["artist_id"]
                    ,track_json[track_key]["artist_name"]
                    ,track_json[track_key]["album_id"]
                    ,track_json[track_key]["track_number"]
                    
                    # feature
                    ,feature["acousticness"]
                    ,feature["analysis_url"]
                    ,feature["danceability"]
                    ,feature["duration_ms"]
                    ,feature["energy"]
                    ,feature["instrumentalness"]
                    ,feature["liveness"]
                    ,feature["loudness"]
                    ,feature["mode"]
                    ,feature["speechiness"]
                    ,feature["tempo"]
                    ,feature["time_signature"]
                    ,feature["valence"]
        ]
        
        wr.writerow(track_result)
        token_queue.put(access_token)
        mylogger.info(f"TRACK DONE [{idx+1}/{len(total_track_list)}] || track_id :: {track_key}")
    f.close()



if __name__ == "__main__" :
    
    # secret json 가져오기 - extract와 동일한 경로에 secret 업로드
    with open('./secret.json', 'r') as jsonfile :
        client_info = json.load(jsonfile)
    
    # main token 
    CLIENT_ID = client_info["client_id"]
    CLIENT_SECRET = client_info["client_secret"]
    
    # 대표 TOKEN
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET) # Artist 추출할 때 처음 지정할 token 생성

    # TOKEN queue 만들기
    token_queue = queue.Queue()
    for client_id, client_secret in client_info["client_info"] :
        token_queue.put(get_access_token(client_id, client_secret))


    # FILE LOCATION 고정 변수
    DATA_PATH = './result/' + ymd + '/'
    ERROR_PATH = './errors/' + ymd + '/'
    artist_path = DATA_PATH + 'kpop_artist_data_1000.csv'                            # artist csv 저장 경로
    artist_album_path = DATA_PATH + 'kpop_artist_album_data.csv'                # artist의 album csv 저장 경로
    artist_album_track_path = DATA_PATH + 'kpop_artist_album_track_data.csv'    # track csv 저장 경로
    os.makedirs(DATA_PATH, exist_ok = True) 

    # Logger
    mylogger = make_log(timestamp)
    
    # extract_track(token_queue)

    
    #####################################################
    #               ARTIST 추출  
    #####################################################   
    if not os.path.exists(DATA_PATH + 'kpop_artist_data.csv'):
        # artist 스크래핑 해오기
        total_artist_list = scraping_kpop_artist()
    else :
        # 만약 스크래핑을 이미 해왔다면, csv를 기준으로 항목 가져오기
        total_artist_list = []
        with open(DATA_PATH + "kpop_artist_data.csv", 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            header = next(csvreader)
            for row in csvreader:
                total_artist_list.append(row[0])
    time.sleep(1)

    # TEST (artist 10명에 대한 album & track 만 가져오기)
    # total_artist_list = total_artist_list[:10]
    
    # KPOP ARTIST ALBUM 정보 - csv 파일 우선 생성
    with open(artist_album_path, 'w', encoding = 'utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['id','name', 'external_url', 'artist_id', 'artist_name', 'image_url', 'release_date', 'total_tracks'])
    
    # KPOP ARTIST ALBUM의 TRACK 정보 - csv 파일 우선 생성
    with open(artist_album_track_path, 'w', encoding = 'utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['id' , 'name', 'track_href','external_url' , 'artist_id', 'artist_name', 'album_id', 'track_number', 'acousticness', 'analysis_url', 'danceability', 'duration_ms', 'energy',  'instrumentalness', 'liveness', 'loudness', 'mode', 'speechiness', 'tempo', 'time_signature', 'valence'])

    
    #################################################
    #                   Thread
    #################################################
    data_q = queue.Queue()
    error_q = queue.Queue()
    
    thread_count = 20
    thread_list = []
    
    work = len(total_artist_list) // thread_count  
    
    for i in range(thread_count) : 
        start =  i * work
        
        if i == thread_count - 1 :
            end = len(total_artist_list)
        else:
            end = (i+1) * work
        thread_list.append(threading.Thread(target=run_thread, args = (start, end, data_q, error_q, token_queue))) 
    
    [thread.start() for thread in thread_list]
    [thread.join() for thread in thread_list]        

    # ERROR 확인
    data_count = data_q.qsize()
    error_count = error_q.qsize()
    
    if error_count != 0 :
        os.makedirs(ERROR_PATH, exist_ok = True)
        with open(ERROR_PATH + 'error_artist_data.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['artist_key', 'err_msg', 'err_lineno'])
            
            while not error_q.empty() :
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
    os.system(f'aws s3 cp {artist_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(f"upload S3 bucket {artist_path} to s3://spotify-kpop-analysis/result_data/")
    os.system(f'aws s3 cp {artist_album_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(f"upload S3 bucket {artist_album_path} to s3 s3://spotify-kpop-analysis/result_data/")
    os.system(f'aws s3 cp {artist_album_track_path} s3://spotify-kpop-analysis/result_data/')
    mylogger.info(f"upload S3 bucket {artist_album_track_path} s3://spotify-kpop-analysis/result_data/")
    mylogger.info(f"DONE")