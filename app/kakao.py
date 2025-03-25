import requests
import json
from typing import List, Dict, Any
from config import KAKAO_REST_API_KEY

class KakaoMessage:
    """
    Handles sending KakaoTalk messages using multiple templates.
    
    This class uses configuration settings from config.json to determine
    the message format, and it automatically refreshes the access token when needed.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes KakaoMessage with app credentials and token management.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary from config.json.
        """
        self.app_key = KAKAO_REST_API_KEY
        self.config = config
        self.template_type = config.get("template", "default_text")
        
        # Load tokens and immediately refresh them.
        with open("kakao_access_token.json", "r") as fp:
            self.tokens = json.load(fp)
        self.refresh_token()

    def refresh_token(self) -> None:
        """
        Refresh the Kakao API access token using the stored refresh token.
        
        The updated tokens are saved back to the kakao_access_token.json file.
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

    def format_message(self, config: Dict[str, Any], keyword: str, papers: List[Dict[str, Any]], top_k: int = 10) -> (str, Dict[str, Any]):
        """
        Format the KakaoTalk message payload based on the selected template.
        
        For the default text template, it builds a simple text message that lists the top papers with their scores.
        For custom templates, it constructs a payload with template arguments.

        Args:
            config (Dict[str, Any]): Configuration dictionary.
            keyword (str): The search keyword.
            papers (List[Dict[str, Any]]): List of papers to include.
            top_k (int, optional): Maximum number of papers to include. Defaults to 10.

        Returns:
            tuple: A tuple containing:
                - url (str): The endpoint URL for sending the message.
                - data (Dict[str, Any]): The payload to be sent to the API.
        """
        
        if self.template_type == "default_text":
            url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
            content = f"🔍 Search Keyword:\n'{keyword}'\n\n"
            for rank, paper in enumerate(papers[:top_k]):
                title = paper.get('title', 'No Title')
                link = paper.get('link', '')
                reranker_score = round(paper.get("reranker_score", 0), 4)
                content += f"[{rank+1}] ({reranker_score}) {title}\n{link}\n\n"
            data = {
                "template_object": json.dumps({
                    "object_type": "text",
                    "text": content,
                    "link": {
                        "web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier",
                        "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"
                    },
                    "buttons": [{"title": "Github","link": {"web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier", "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"}}, 
                                {"title": "Project Page", "link": {"web_url": "https://jonghwi-kim.github.io/", "mobile_url": "https://jonghwi-kim.github.io/"}}]
                })
            }
            return url, data
        else:
            url = "https://kapi.kakao.com/v2/api/talk/memo/send"
            template_arguments = {
                "SEARCH_QUERY": keyword,
                "N_PAPERS": str(len(papers))
            }
            for rank, paper in enumerate(papers[:top_k]):
                template_arguments[f"TITLE_{rank+1}"] = paper.get('title', 'No Title')
                # Use only the last segment of the link for brevity.
                template_arguments[f"LINK_{rank+1}"] = paper.get('link', '').split("/")[-1]
            return url, {
                "template_id": config.get("template_id", ""),
                "template_args": json.dumps(template_arguments, ensure_ascii=False)
            }

    def send_paper_kakao(self, config: Dict[str, Any], keyword: str, papers: List[Dict[str, Any]], top_k: int = 10) -> requests.Response:
        """
        Send a KakaoTalk message using the configured template.
        
        Depending on the template type and availability of papers, it formats the message accordingly.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary containing system settings.
            keyword (str): The search keyword.
            papers (List[Dict[str, Any]]): List of relevant papers.
            top_k (int, optional): Maximum number of papers to include. Defaults to 10.
        
        Returns:
            requests.Response: The HTTP response from the KakaoTalk API.
        """
        headers = {"Authorization": "Bearer " + self.tokens['access_token']}
        buttons = [{"title": "Github","link": {"web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier", "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"}},
                   {"title": "Project Page", "link": {"web_url": "https://jonghwi-kim.github.io/", "mobile_url": "https://jonghwi-kim.github.io/"}}]
        if papers:
            url, data = self.format_message(config, keyword, papers, top_k)
        else:
            url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
            content = f"📢 No new updates for '{keyword}'."
            data = {"template_object": json.dumps({
                    "object_type": "text",
                    "text": content,
                    "link": {
                        "web_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier",
                        "mobile_url": "https://github.com/jonghwi-kim/arXiv_paper_notifier"
                    },
                    "buttons": buttons
                })
            }
        response = requests.post(url, headers=headers, data=data)
        return response
