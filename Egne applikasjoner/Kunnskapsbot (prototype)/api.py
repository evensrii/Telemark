"""
Local prototype API for the Kunnskapsbot: retrieves relevant chunks from
index.npz and asks Azure OpenAI to answer questions grounded in them.

Run: uvicorn api:app --reload
"""

import os

import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from openai import AzureOpenAI
from pydantic import BaseModel

load_dotenv(os.path.join(os.environ["PYTHONPATH"], "token.env"))

client = AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    api_version=os.environ["AZURE_OPENAI_API_VERSION"],
)
EMBEDDING_DEPLOYMENT = os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"]
CHAT_DEPLOYMENT = os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT"]

TOP_K = 5
SYSTEM_PROMPT = (
    "Du er en assistent som svarer på spørsmål om Telemark fylke, basert utelukkende "
    "på utdragene under. Hvis svaret ikke finnes i utdragene, si at du ikke vet det "
    "i stedet for å gjette. Svar på norsk."
)

index = np.load("index.npz", allow_pickle=True)
VECTORS = index["vectors"]
URLS = index["urls"]
TITLES = index["titles"]
TEXTS = index["texts"]

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    question: str


def embed_query(text):
    response = client.embeddings.create(model=EMBEDDING_DEPLOYMENT, input=[text])
    return np.array(response.data[0].embedding, dtype=np.float32)


def top_k_chunks(query_vector, k=TOP_K):
    norms = np.linalg.norm(VECTORS, axis=1) * np.linalg.norm(query_vector)
    similarities = VECTORS @ query_vector / np.where(norms == 0, 1, norms)
    top_indices = np.argsort(similarities)[::-1][:k]
    return [
        {"url": str(URLS[i]), "title": str(TITLES[i]), "text": str(TEXTS[i])}
        for i in top_indices
    ]


@app.post("/chat")
def chat(request: ChatRequest):
    query_vector = embed_query(request.question)
    chunks = top_k_chunks(query_vector)

    context = "\n\n---\n\n".join(f"Fra {c['title']} ({c['url']}):\n{c['text']}" for c in chunks)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Utdrag:\n\n{context}\n\nSpørsmål: {request.question}"},
    ]

    response = client.chat.completions.create(model=CHAT_DEPLOYMENT, messages=messages)
    answer = response.choices[0].message.content

    return {
        "answer": answer,
        "sources": [{"url": c["url"], "title": c["title"]} for c in chunks],
    }
