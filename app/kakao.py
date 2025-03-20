import requests
import json
from config import KAKAO_REST_API_KEY

class KakaoMessage():
    def __init__(self,):
        self.app_key = KAKAO_REST_API_KEY

        with open("kakao_access_token.json", "r") as fp:
            self.tokens = json.load(fp)
            self.refresh_token()

    def refresh_token(self):
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.app_key,
            "refresh_token": self.tokens['refresh_token']
            }

        response = requests.post(url, data=data)

        # 갱신 된 토큰 내용 확인
        result = response.json()

        # 갱신 된 내용으로 파일 업데이트
        if 'access_token' in result:
            self.tokens['access_token'] = result['access_token']

        if 'refresh_token' in result:
            self.tokens['refresh_token'] = result['refresh_token']
        else:
            pass

        with open("kakao_access_token.json", "w") as fp:
            json.dump(self.tokens, fp)

    def send_paper_custom_kakao(self, keyword, papers, top_k=5):
        url = "https://kapi.kakao.com/v2/api/talk/memo/send"

        headers = {"Authorization": "Bearer " + self.tokens['access_token']}
        
        template_arguments = {"SEARCH_QUERY" : keyword, 
                              "N_PAPERS" : str(len(papers))}
        
        for rank, paper in enumerate(papers[:top_k]):
            template_arguments[f"TITLE_{rank+1}"] = paper['title'].replace("\n", "")
            template_arguments[f"LINK_{rank+1}"] = paper['link'].split("/")[-1]
    
        data = {
            "template_id":"118367",
            "template_args": json.dumps(template_arguments, ensure_ascii=False)
        }

        print(data)

        response = requests.post(url, headers=headers, data=data)
        return response
    
    def send_paper_default_kakao(self, keyword, papers, top_k=5):
        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"

        headers = {"Authorization": "Bearer " + self.tokens['access_token']}
        
        content = f"검색어: {keyword}\n\n"

        for rank, paper in enumerate(papers[:top_k]):
            title = paper['title'].replace("\n", "")
            link = paper['link']
            content += f"[{rank+1}] {title}\n"
            content += f"{link}\n\n"
    
        data={
            "template_object": json.dumps({
                "object_type":"text",
                "text":content,
                "link":{
                    "web_url":"https://github.com/jonghwi-kim/arXiv_paper_notifier",
                    "mobile_url":"https://github.com/jonghwi-kim/arXiv_paper_notifier"
                },
                "buttons": [
                    {"title":"Github", "link":{"web_url":"https://github.com/jonghwi-kim/arXiv_paper_notifier","mobile_url":"https://github.com/jonghwi-kim/arXiv_paper_notifier"}},
                    {"title":"Project Page", "link":{"web_url":"https://jonghwi-kim.github.io/","mobile_url":"https://jonghwi-kim.github.io/"}}
                ]
            })
        }

        response = requests.post(url, headers=headers, data=data)
        return response