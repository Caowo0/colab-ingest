# BuzzHeavier Downloader

## Simple downloader for filehosting service [BuzzHeavier](https://buzzheavier.com/), written in Python.

Supports batch downloads from a text file, and all [BuzzHeavier mirrors](https://buzzheavier.com/proxy).

---

## Requirements

- Python 3.x
- `requests` and `beautifulsoup4` libraries
- `tqdm` for the progress bar

Install dependencies:

```bash
pip install requests beautifulsoup4 tqdm
```

---

## Usage

### 1. Download by ID or URL

> [!WARNING]  
> This tool does not yet support downloading from directories, only files.

```bash
python3 bhdownload.py <id_or_url>
```

**Examples:**

```bash
python3 bhdownload.py https://buzzheavier.com/x7v9k2mqp4zt
python3 bhdownload.py x7v9k2mqp4zt
```

### 2. Batch Download from a Text File

Prepare a text file (`input.txt`) with one ID or full URL per line:

```
x7v9k2mqp4zt
https://bzzhr.co/x7v9k2mqp4zt
https://buzzheavier.com/x7v9k2mqp4zt
```

Then run:

```bash
python3 bhdownload.py -f input.txt
```

---

## Debug Info

The script prints the following debug info:

- `[DEBUG] Title:` shows the filename it will use.
- `[DEBUG] Download link:` shows the internal redirect used to fetch the actual file.
- Download progress bar.

---

## Notes

- All domains are interchangeable — the script handles them automatically.
- Make sure IDs are exactly 12 characters or full URLs from supported domains.

---

## No License

Use this code however the fuck you want. With good intent or not, for profit or not. It's public domain!

---
> [!TIP]
> Star this repo if you got baited by the link and opened it... it's fake ;)
