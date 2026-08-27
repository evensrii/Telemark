"""
Chunk the crawled pages from pages.json plus the structured datasets from
data_pages.json (see load_data_files.py - numbers shown as charts on the
site aren't visible to crawl.py, so they're fed in separately from the
repo's own Data/ CSVs), embed each chunk via Azure OpenAI, and save the
resulting chunks + vectors to index.npz.

Run: python build_index.py
"""

import json
import os

import numpy as np
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv(os.path.join(os.environ["PYTHONPATH"], "token.env"))

client = AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    api_version=os.environ["AZURE_OPENAI_API_VERSION"],
)
EMBEDDING_DEPLOYMENT = os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"]

PAGES_FILE = "pages.json"
DATA_PAGES_FILE = "data_pages.json"
INDEX_FILE = "index.npz"
CHUNK_SIZE_CHARS = 2000  # roughly 400-500 tokens
CHUNK_OVERLAP_CHARS = 200


def chunk_text(text):
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE_CHARS
        chunks.append(text[start:end])
        start = end - CHUNK_OVERLAP_CHARS
    return chunks


def embed(texts):
    response = client.embeddings.create(model=EMBEDDING_DEPLOYMENT, input=texts)
    return [item.embedding for item in response.data]


def load_pages(path):
    if not os.path.exists(path):
        print(f"{path} not found, skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_index():
    pages = load_pages(PAGES_FILE) + load_pages(DATA_PAGES_FILE)

    chunk_records = []
    for page in pages:
        for chunk in chunk_text(page["text"]):
            if chunk.strip():
                chunk_records.append({"url": page["url"], "title": page["title"], "text": chunk})

    print(f"Embedding {len(chunk_records)} chunks from {len(pages)} pages...")
    vectors = []
    batch_size = 16
    for i in range(0, len(chunk_records), batch_size):
        batch = chunk_records[i : i + batch_size]
        vectors.extend(embed([c["text"] for c in batch]))
        print(f"  embedded {min(i + batch_size, len(chunk_records))}/{len(chunk_records)}")

    np.savez(
        INDEX_FILE,
        vectors=np.array(vectors, dtype=np.float32),
        urls=np.array([c["url"] for c in chunk_records]),
        titles=np.array([c["title"] for c in chunk_records]),
        texts=np.array([c["text"] for c in chunk_records]),
    )
    print(f"Saved index with {len(chunk_records)} chunks to {INDEX_FILE}")


if __name__ == "__main__":
    build_index()
