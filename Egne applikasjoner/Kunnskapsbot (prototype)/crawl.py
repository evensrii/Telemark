"""
Crawl https://www.telemarkfylke.no/no/kunnskap-om-telemark/ and its subpages,
extract the main article text from each page, and save the result to pages.json.

Run: python crawl.py
"""

import json
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT_URL = "https://www.telemarkfylke.no/no/kunnskap-om-telemark/"
OUTPUT_FILE = "pages.json"
REQUEST_TIMEOUT = 15
HEADERS = {"User-Agent": "Mozilla/5.0 (KunnskapsbotPrototype/1.0)"}


def is_in_scope(url):
    parsed = urlparse(url)
    root_parsed = urlparse(ROOT_URL)
    return parsed.netloc == root_parsed.netloc and parsed.path.startswith(root_parsed.path)


def extract_text(soup):
    for tag in soup.select("script, style, nav, header, footer, noscript"):
        tag.decompose()
    main = soup.select_one("main") or soup.select_one("article") or soup.body
    if main is None:
        return ""
    text = main.get_text(separator="\n", strip=True)
    return "\n".join(line for line in text.splitlines() if line.strip())


def crawl():
    visited = set()
    queue = deque([ROOT_URL])
    pages = []

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Skipping {url}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else url
        text = extract_text(soup)

        if text:
            pages.append({"url": url, "title": title, "text": text})
            print(f"Crawled ({len(pages)}): {url}")

        for link in soup.find_all("a", href=True):
            absolute_url = urljoin(url, link["href"]).split("#")[0]
            if is_in_scope(absolute_url) and absolute_url not in visited:
                queue.append(absolute_url)

    return pages


if __name__ == "__main__":
    pages = crawl()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(pages)} pages to {OUTPUT_FILE}")
