from time import time
import logging
from opensearchpy import helpers, OpenSearch

def create_index(client, index_name, index_body):
    """
    신규 인덱스를 정의한다
    """
    #logging.info("start create index")

    # index creation
    if not (client.indices.exists(index_name)):
        logging.info("[create_index] create new index")
        client.indices.create(index=index_name, body=index_body)
        print(index_name)

def remove_index(client, index_name):
    """
    인덱스를 삭제한다
    """
    if client.indices.exists(index_name):
        client.indices.delete(index=index_name)
        logging.info(f"[remove_index] remove_index {index_name}")

def gen_data():
    pass

def update_data(opensearch, index_name, basetime, number_of_shards):
    """
    신규 데이터를 지정한 index_name으로 집어 넣는다.
    """
    start_time = time.time()
    resp = helpers.bulk(
        opensearch, gen_data(index_name, basetime), chunk_size=4000, request_timeout=300
    )
    inference_time = time.time() - start_time
    return resp, inference_time