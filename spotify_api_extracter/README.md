# Spotify API를 이용하여 K-POP ARTIST에 해당하는 정보 가져오기

### 필요한 라이브러리
```
pip3 install spotify
```

### S3 CLI 방법

* AWS IAM 
    * 사용자 (내 계정)
    * 보안 자격 증명
    * 액세스 키
    * 액세스 키 만들기
    * access_key와 access_secret 발급 받기
* CLI
    * [AWS Command Line Interface](https://aws.amazon.com/ko/cli/)
    * 운영체제 별 다운로드
* cmd 
    * `aws configure`
    * 발급 받았던 access_key, access_secret 등록
        * region이나 기타 내용 안쓰고 엔터 쳐도 된다
    * `aws s3 ls` 명령어로 Test
