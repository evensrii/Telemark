import requests

class YodaTranslator:
    def __init__(self):
        self.base_url = "https://api.funtranslations.com/translate/yoda.json"

    def translate(self, text):
        try:
            response = requests.post(self.base_url, data={"text": text})
            response.raise_for_status()
            
            data = response.json()
            return data["contents"]["translated"]
        except requests.exceptions.RequestException as e:
            return f"Error during translation: {str(e)}"
        except (KeyError, ValueError) as e:
            return f"Error processing response: {str(e)}"

def main():
    translator = YodaTranslator()
    
    print("Welcome to Yoda Translator!")
    print("Note: Free API is limited to 60 calls per day (5 per hour)")
    
    while True:
        text = input("\nEnter text to translate (or 'quit' to exit): ")
        if text.lower() == 'quit':
            break
            
        translated = translator.translate(text)
        print(f"\nYoda says: {translated}")

if __name__ == "__main__":
    main()
