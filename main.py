from opensearchpy import connections, helpers, RequestsHttpConnection, AWSV4SignerAuth
from opensearchpy import exceptions
from decouple import config
from boto3.session import Session

SRC_INDEX_NAME = config("SRC_INDEX_NAME")
DEST_INDEX_NAME = config("DEST_INDEX_NAME")
READ_BATCH_SIZE = 500

def create_connections():
    session = Session(aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                                   aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
                                   region_name=config('AWS_REGION'))
    auth = AWSV4SignerAuth(
        credentials=session.get_credentials(),
        region=config("OPENSEARCH_REGION"),
        service=config("OPENSEARCH_SERVICE_NAME"),
    )

    connections.create_connection(
        alias="aws",
        hosts=[{"host": config("OPENSEARCH_HOST"), "port": config("OPENSEARCH_PORT")}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    connections.create_connection(
        alias="local",
        hosts=[{"host": config('LOCAL_OPENSEARCH_HOST'), "port": config('LOCAL_OPENSEARCH_PORT')}],
        use_ssl=False,
        verify_certs=False,
    )

def get_clients():
    src = connections.get_connection(alias="aws")
    dst = connections.get_connection(alias="local")
    return src, dst

def _unwrap_typeless_mappings(m):
    if "mappings" in m:
        m = m["mappings"]

    # unwrap old-typed mappings like {"_doc": {...}}
    if isinstance(m, dict) and len(m) == 1 and list(m.keys())[0].startswith("_"):
        m = list(m.values())[0]
    return m

def _clean_knn_params(m):
    props = m.get("properties", {})
    def cleanse(pdict):
        for k, v in list(pdict.items()):
            if isinstance(v, dict):
                t = v.get("type")
                if t in ("knn_vector", "vector", "text_embedding"):
                    v.pop("model_id", None)
                    v.pop("modelId", None)
                    v.pop("method", None)
                if "properties" in v:
                    cleanse(v["properties"])
        return pdict
    m["properties"] = cleanse(props)
    return m

def ensure_destination_index(dst_client, src_client):
    exists = dst_client.indices.exists(index=DEST_INDEX_NAME)
    print(f"destination index exists={exists}")
    if exists:
        return
    src_mapping_all = src_client.indices.get_mapping(index=SRC_INDEX_NAME)
    src_mapping = src_mapping_all.get(SRC_INDEX_NAME, {})

    mappings = _unwrap_typeless_mappings(src_mapping)
    mappings = _clean_knn_params(mappings if isinstance(mappings, dict) else {})

    settings = {
        "settings": {
            "index": {
                "knn": True
            }
        },
        "mappings": mappings
    }

    try:
        dst_client.indices.create(index=DEST_INDEX_NAME, body=settings)
        print("destination index created with index.knn=true")
    except exceptions.RequestError as e:
        print(f"index create failed: {e}")

def source_query_body():
    return {
        "query": {
            "exists": {"field": "is_ogm_materyal"}
        },
        "sort": [{"_id": "asc"}]
    }

def iter_source_hits(src_client, batch_size):
    body = source_query_body()
    resp = src_client.search(index=SRC_INDEX_NAME, body=body, size=batch_size)
    hits = resp.get("hits", {}).get("hits", []) or []
    while hits:
        for h in hits:
            yield h
        last_sort = hits[-1].get("sort")
        if not last_sort:
            break
        body["search_after"] = last_sort
        resp = src_client.search(index=SRC_INDEX_NAME, body=body, size=batch_size)
        hits = resp.get("hits", {}).get("hits", []) or []

def to_bulk_actions(hits):
    for h in hits:
        yield {
            "_op_type": "index",
            "_index": DEST_INDEX_NAME,
            "_id": h["_id"],
            "_source": h.get("_source", {})
        }

def transfer(batch_size=READ_BATCH_SIZE, dry_run=False):
    src, dst = get_clients()
    ensure_destination_index(dst, src)

    buffer = []
    total_sent = 0
    for hit in iter_source_hits(src, batch_size):
        buffer.append(hit)
        if len(buffer) >= batch_size:
            print(f"bulk write start count={len(buffer)} total_sent={total_sent}")
            if not dry_run:
                helpers.bulk(dst, to_bulk_actions(buffer), request_timeout=120)
            total_sent += len(buffer)
            print(f"bulk write done batch_count={len(buffer)} total_sent={total_sent}")
            buffer = []
    if buffer:
        print(f"bulk write start count={len(buffer)} total_sent={total_sent}")
        if not dry_run:
            helpers.bulk(dst, to_bulk_actions(buffer), request_timeout=120)
        total_sent += len(buffer)
        print(f"bulk write done batch_count={len(buffer)} total_sent={total_sent}")
    print(f"transfer finished total_sent={total_sent}")

if __name__ == "__main__":
    create_connections()
    transfer()
