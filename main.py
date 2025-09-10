from opensearchpy import connections, helpers, RequestsHttpConnection, AWSV4SignerAuth
from opensearchpy import exceptions
from decouple import config
from boto3.session import Session

INDEX_NAME = "bedrock-knowledge-base-default-index"
MAX_DOC = 100
READ_BATCH_SIZE = 200

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
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
    )

def get_clients():
    src = connections.get_connection(alias="aws")
    dst = connections.get_connection(alias="local")
    return src, dst

def _unwrap_typeless_mappings(mappings):
    if "mappings" in mappings:
        mappings = mappings["mappings"]
    # unwrap old-typed mappings like {"_doc": {...}}
    if isinstance(mappings, dict) and len(mappings) == 1 and list(mappings.keys())[0].startswith("_"):
        mappings = list(mappings.values())[0]
    return mappings

def _clean_knn_params(mappings):
    # ensure we don't carry serverless-only knobs that could fail even with index.knn=true
    props = mappings.get("properties", {})
    def cleanse(pdict):
        for k, v in pdict.items():
            if isinstance(v, dict):
                t = v.get("type")
                if t in ("knn_vector", "vector", "text_embedding"):
                    v.pop("model_id", None)
                    v.pop("modelId", None)
                    v.pop("method", None)
                if "properties" in v:
                    cleanse(v["properties"])
        return pdict
    mappings["properties"] = cleanse(props)
    return mappings

def ensure_destination_index_v2(dst_client, src_client, index_name):
    index_name_local = f'{index_name}_local'
    exists = dst_client.indices.exists(index=index_name_local)
    print(f"destination index exists={exists}")
    if exists:
        return
    src_mapping = src_client.indices.get_mapping(index=index_name)
    mappings = src_mapping[index_name]["mappings"] if index_name in src_mapping else {}
    print("has mapping: ",  mappings is not None)
    # dst_client.indices.create(index=index_name_local, body={"mappings": mappings})
    print("destination index created")

def ensure_destination_index(dst_client, src_client, index_name):
    index_name_local = f'{index_name}_local'
    exists = dst_client.indices.exists(index=index_name_local)
    print(f"destination index exists={exists}")
    if exists:
        return
    src_mapping_all = src_client.indices.get_mapping(index=index_name)
    src_mapping = src_mapping_all.get(index_name, {})
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
        # print(mappings)
        dst_client.indices.create(index=index_name_local, body=settings)
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

def iter_source_hits(src_client, index_name, batch_size):
    body = source_query_body()
    resp = src_client.search(index=index_name, body=body, size=batch_size)
    hits = resp.get("hits", {}).get("hits", []) or []
    while hits:
        for h in hits:
            yield h
        last_sort = hits[-1].get("sort")
        if not last_sort:
            break
        body["search_after"] = last_sort
        resp = src_client.search(index=index_name, body=body, size=batch_size)
        hits = resp.get("hits", {}).get("hits", []) or []

def to_bulk_actions(hits, dest_index):
    print('desct index: ', dest_index)
    for h in hits:
        yield {
            "_op_type": "index",
            "_index": dest_index,
            "_id": h["_id"],
            "_source": h.get("_source", {})
        }

def transfer(index_name=INDEX_NAME, batch_size=READ_BATCH_SIZE, dry_run=False):
    src, dst = get_clients()
    index_name_local = f"{index_name}_local"
    ensure_destination_index(dst, src, index_name)
    # return
    buffer = []
    total_sent = 0
    i = 0
    for hit in iter_source_hits(src, index_name, batch_size):
        i += 1
        buffer.append(hit)

        if i >= MAX_DOC:
            break

        if len(buffer) >= batch_size:
            print(f"bulk write start count={len(buffer)} total_sent={total_sent}")
            if not dry_run:
                helpers.bulk(dst, to_bulk_actions(buffer, index_name_local), request_timeout=120)
            total_sent += len(buffer)
            print(f"bulk write done batch_count={len(buffer)} total_sent={total_sent}")
            buffer = []
        

    print("for finish..")
    if buffer:
        connections.remove_connection('aws')
        print(dst.info())
        print(f"bulk write start count={len(buffer)} total_sent={total_sent}")
        if not dry_run:
            helpers.bulk(dst, to_bulk_actions(buffer, index_name_local), request_timeout=120)
        total_sent += len(buffer)
        print(f"bulk write done batch_count={len(buffer)} total_sent={total_sent}")
    print(f"transfer finished total_sent={total_sent}")

if __name__ == "__main__":
    create_connections()
    transfer()
