#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import sys
import os
from tqdm import tqdm

VALID_DOMAINS = [
    'buzzheavier.com',
    'bzzhr.co',
    'fuckingfast.net',
    'fuckingfast.co'
]

def resolve_url(input_str):
    input_str = input_str.strip()
    if input_str.startswith('http'):
        for domain in VALID_DOMAINS:
            if domain in input_str:
                return input_str
        raise ValueError(f"URL domain not recognized: {input_str}")
    elif len(input_str) == 12:
        return f'https://{VALID_DOMAINS[0]}/{input_str}'
    else:
        raise ValueError(f"Invalid input: {input_str}")

def download_buzzheavier(input_str):
    url = resolve_url(input_str)

    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    title = soup.title.string.strip()
    print(f"[DEBUG] Title: {title}")

    download_url = url + '/download'
    headers = {
        'hx-current-url': url,
        'hx-request': 'true',
        'referer': url
    }
    head_response = requests.head(download_url, headers=headers, allow_redirects=False)
    hx_redirect = head_response.headers.get('hx-redirect')
    if not hx_redirect:
        raise Exception("Download link not found. Is this a directory?")
    print(f"[DEBUG] Download link: {hx_redirect}")

    domain = url.split('/')[2]
    final_url = f'https://{domain}' + hx_redirect if hx_redirect.startswith('/dl/') else hx_redirect
    file_response = requests.get(final_url, stream=True)
    file_response.raise_for_status()

    total_size = int(file_response.headers.get('content-length', 0))
    block_size = 1024
    with open(title, 'wb') as f, tqdm(
        total=total_size, unit='B', unit_scale=True, desc=title
    ) as progress_bar:
        for chunk in file_response.iter_content(chunk_size=block_size):
            if chunk:
                f.write(chunk)
                progress_bar.update(len(chunk))
    print(f"Downloaded as: {title}")

def process_input():
    args = sys.argv[1:]
    if not args:
        print("Usage:\nbhdownload.py <id_or_url>\nbhdownload.py -f <file_with_ids_or_urls>")
        return

    if args[0] == '-f' and len(args) > 1:
        file_path = args[1]
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line:
                    try:
                        download_buzzheavier(line)
                    except Exception as e:
                        print(f"Failed to download {line}: {e}")
    else:
        try:
            download_buzzheavier(args[0])
        except Exception as e:
            print(f"Failed to download {args[0]}: {e}")

if __name__ == '__main__':
    process_input()
