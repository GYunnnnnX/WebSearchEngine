from elasticsearch import Elasticsearch, helpers
import json
import argparse
from pathlib import Path

#将所有网页jsonl数据批量导入到Elasticsearch对应的索引中

#相对地址
current_dir = Path(__file__).parent

#构造JSONL文件的路径
document_jsonl_path = current_dir.parent / 'data' / 'document_data' / 'document_all.jsonl'
web_jsonl_path = current_dir.parent / 'data' / 'web_data' / 'parsed_all.jsonl'

#导入到web_search_engine索引中
def index_web_pages(jsonl_file,es):
    actions = []
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line in f:
            doc = json.loads(line)
            action = {
                "_index": "web_search_engine",
                "_id": doc["id"],
                "_source": {
                    "id": doc["id"],
                    "url": doc["url"],
                    "title": doc["title"],
                    "content": doc["content"],
                    "anchors": doc["anchors"],
                    "snapshot_path": doc["snapshot_path"],
                    "doc_type": "webpage"
                }
            }
            actions.append(action)
    
    #helpers.bulk(es, actions)
    success, errors = helpers.bulk(es, actions, stats_only=False, raise_on_error=False)
    if errors:
        print(f"导入失败文档数: {len(errors)}")
        for error in errors:
            print(f"文档ID: {error['index']['_id']}")
            print(f"错误原因: {error['index']['error']['reason']}")
            print("--" * 20)
    else:
        print(f"成功导入 {success} 个文档")

#导入到web_search_engine_document索引中
def index_document_pages(jsonl_file,es):
    actions = []
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line in f:
            doc = json.loads(line)
            action = {
                "_index": "web_search_engine_document",
                "_id": doc["id"],
                "_source": {
                    "id": doc["id"],
                    "url": doc["url"],
                    "title": doc["title"],
                    "original_filename": doc["original_filename"],
                    "doc_type": "document"
                }
            }
            actions.append(action)
    
    helpers.bulk(es, actions)


if __name__ == "__main__":
    es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "=j2jb70ynctOB8fIaOdi"), 
    verify_certs=False  # 跳过证书验证（相当于 curl -k）
    )
    #导入数据
    index_web_pages(web_jsonl_path, es)
    index_document_pages(document_jsonl_path, es)