# Export index from Amazon OpenSearch Serverless to Managed Open Source

## Requirements

* Python

## Setup

* Copy env file:

  ```bash
  cp .env.example .env
  ```
* Update `.env` with your values.

## Run

1. Install libraries

```bash
pip install -r requirements.txt
```

2. Dry run (no inserts)

```bash
python main.py --dry-run
```

> Creates `DEST_INDEX_NAME` with mappings, skips document inserts.

3. Execute transfer

```bash
python main.py
```
