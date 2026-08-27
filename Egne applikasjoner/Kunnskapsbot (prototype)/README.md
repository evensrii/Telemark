# Kunnskapsbot (prototype)

Free-standing, local-only prototype of a RAG chatbot that answers questions about
Telemark based on the content of https://www.telemarkfylke.no/no/kunnskap-om-telemark/
and its subpages. Not connected to the `bothotell`/`privatistbot` widget used elsewhere
on telemarkfylke.no — this is a separate, standalone experiment.

## Prerequisites

- The `analyse` conda environment, updated with the extra dependencies below.
- An Azure OpenAI resource with a chat model deployment (e.g. `gpt-4o-mini`) and an
  embedding model deployment (e.g. `text-embedding-3-small`) already provisioned.
- The following added to `Python/token.env`:

  ```
  AZURE_OPENAI_ENDPOINT=https://<your-resource>.openai.azure.com/
  AZURE_OPENAI_API_KEY=<your-key>
  AZURE_OPENAI_API_VERSION=2024-08-01-preview
  AZURE_OPENAI_CHAT_DEPLOYMENT=<your-chat-deployment-name>
  AZURE_OPENAI_EMBEDDING_DEPLOYMENT=<your-embedding-deployment-name>
  ```

## Setup

```
conda update --file ../../Python/environment.yaml --prune
conda activate analyse
```

## Run the prototype

From this folder:

1. **Crawl the site**
   ```
   python crawl.py
   ```
   Produces `pages.json` with the text of every page under `/no/kunnskap-om-telemark/`.

2. **Build the embedding index**
   ```
   python build_index.py
   ```
   Produces `index.npz`.

3. **Start the API**
   ```
   uvicorn api:app --reload
   ```
   Serves `POST /chat` on `http://localhost:8000`.

4. **Try the widget**
   Open `widget/index.html` directly in a browser (double-click it, or serve it with
   any static file server) and chat with the bot in the bottom-right widget.

## Notes

- This is a dev-only prototype: no auth, no rate limiting, no deployment. Re-run steps
  1–2 whenever you want to refresh the content.
- Going from here to a real embed later means deploying `api.py` somewhere public and
  pointing `widget.js`'s `data-api-url` at that URL — the widget itself doesn't change.
