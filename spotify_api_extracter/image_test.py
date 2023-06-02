import requests
from PIL import Image
from io import BytesIO

s3_url = "s3://spotify-kpop-analysis/image/"

url = "https://i.scdn.co/image/ab67616d0000b2731ea977fb83d93e179882f643"

res = requests.get(url)
print(res.status_code)

img = Image.open(BytesIO(res.content))
img = img.resize((128,128))
img.show()
img.save('./test.jpg', 'JPEG', qualty = 85)