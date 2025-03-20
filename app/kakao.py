import requests
import json
from typing import List, Dict, Any
from config import KAKAO_REST_API_KEY

class KakaoMessage():
    """
    Handles KakaoTalk message sending with support for multiple templates.
    - Uses config.json to determine the message format.
    - Automatically refreshes the access token when needed.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes KakaoMessage with app credentials and token management.

        :param config: Configuration dictionary from config.json.
        """
        
        self.app_key = KAKAO_REST_API_KEY
        self.config = config
        self.template_type = config.get("template", "default_text")

        with open("kakao_access_token.json", "r") as fp:
            self.tokens = json.load(fp)
            self.refresh_token()

    def refresh_token(self) -> None:
        """
        Refreshes the Kakao API access token using the stored refresh token.
        Saves the updated token back to the JSON file.
        """
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.app_key,
            "refresh_token": self.tokens['refresh_token']
        }

        response = requests.post(url, data=data)
        result = response.json()

        if 'access_token' in result:
            self.tokens['access_token'] = result['access_token']
        if 'refresh_token' in result:
            self.tokens['refresh_token'] = result['refresh_token']

        with open("kakao_access_token.json", "w") as fp:
            json.dump(self.tokens, fp)

    def format_message(self, 
                       config: Dict[str, Any],
                       keyword: str, 
                       papers: List[Dict[str, Any]], 
                       top_k: int = 10
                       ) -> Dict[str, Any]:
        """
        Formats the message payload based on the selected template type.

        :param keyword: The search keyword.
        :param papers: List of papers to include in the notification.
        :param top_k: Maximum number of papers to include.
        :return: Formatted Kakao message payload.
        """
        if self.template_type == "default_text":

            url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
            # Simple text-based message format
            content = f"🔍 Search Keyword: \n{keyword}\n\n"

            for rank, paper in enumerate(papers[:top_k]):
                title = paper['title'].replace("\n", "")
                link = paper['link']
                content += f"[{rank+1}] {title}\n{link}\n\n"

            data = {"template_object": json.dumps({
                    "object_type": "text",
                    "text": content,
                    "link": {
                        "web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier",
                        "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"
                    },
                    "buttons": [
                        {"title": "Github", "link": {"web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier", "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"}},
                        {"title": "Project Page", "link": {"web_url": "https://jonghwi-kim.github.io/", "mobile_url": "https://jonghwi-kim.github.io/"}}
                    ]
                })
                }

            return url, data
        else:
            url = "https://kapi.kakao.com/v2/api/talk/memo/send"
            # Custom template format
            template_arguments = {
                "SEARCH_QUERY": keyword,
                "N_PAPERS": str(len(papers))
            }
            for rank, paper in enumerate(papers[:top_k]):
                template_arguments[f"TITLE_{rank+1}"] = paper['title'].replace("\n", "")
                template_arguments[f"LINK_{rank+1}"] = paper['link'].split("/")[-1]

            return url, {
                "template_id": config["template_id"],
                "template_args": json.dumps(template_arguments, ensure_ascii=False)
            }

    def send_paper_kakao(self, 
                         config: Dict[str, Any],
                         keyword: str, 
                         papers: List[Dict[str, Any]], 
                         top_k: int = 5
                         ) -> requests.Response:
        """
        Sends a KakaoTalk message based on the configured template type.

        :param config: Configuration dictionary containing system settings.
        :param keyword: The search keyword for which papers were found.
        :param papers: List of relevant papers.
        :param top_k: Maximum number of papers to include in the message.
        :return: Response from the KakaoTalk API.
        """
        headers = {"Authorization": "Bearer " + self.tokens['access_token']}
        url, data = self.format_message(config, keyword, papers, top_k)

        response = requests.post(url, headers=headers, data=data)
        return response