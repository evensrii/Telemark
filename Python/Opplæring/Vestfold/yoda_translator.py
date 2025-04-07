import requests
import json
from typing import Optional, Dict, Any

class YodaTranslator:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Yoda translator.
        
        Args:
            api_key (Optional[str]): API key for premium access. Not required for free tier.
        """
        self.base_url = "https://api.funtranslations.com/translate/yoda.json"
        self.api_key = api_key
        
    def translate(self, text: str) -> Dict[str, Any]:
        """Translate text to Yoda speech.
        
        Args:
            text (str): The text to translate
            
        Returns:
            Dict containing the translation response
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        headers = {}
        if self.api_key:
            headers['X-FunTranslations-Api-Secret'] = self.api_key
            
        try:
            response = requests.post(
                self.base_url,
                data={"text": text},
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            if response.status_code == 429:
                print("Rate limit exceeded. Free tier allows 5 calls per hour, 60 calls per day.")
            return {"error": str(e)}

def main():
    # Create translator instance (without API key for free tier)
    translator = YodaTranslator()
    
    print("Welcome to the Yoda Translator!")
    print("Note: Free tier is limited to 5 calls per hour, 60 calls per day.")
    print("Type 'quit' to exit.")
    print("-" * 50)
    
    while True:
        text = input("\nEnter text to translate: ").strip()
        
        if text.lower() == 'quit':
            break
            
        if not text:
            print("Please enter some text to translate.")
            continue
            
        result = translator.translate(text)
        
        if "success" in result:
            translated = result["contents"]["translated"]
            print(f"\nYoda says: {translated}")
        else:
            print(f"\nError: {result.get('error', 'Unknown error occurred')}")

if __name__ == "__main__":
    main()
