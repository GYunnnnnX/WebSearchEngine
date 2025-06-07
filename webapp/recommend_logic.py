# recommend_logic.py
from elasticsearch import Elasticsearch
import hashlib
import elastic_utils
import re

def recommend_for_user(es: Elasticsearch, user: str, top_k=5):
    try:
        user_doc = es.get(index="users_indices", id=user)
        history = elastic_utils.get_user_history(es, user)
        print("用户搜索历史：", history)
        from collections import Counter

        print("提取关键词：")
        common = Counter(history).most_common(3)
        print("常用关键词：", common)

        recommended_results = []
        seen_ids = set()

        for kw, _ in common:
            query = {
                "bool": {
                    "must": [
                        {"multi_match": {"query": kw, "fields": ["title", "content"]}}
                    ]
                }
            }

            resp = es.search(index="web_search_engine", body={"query": query}, size=top_k)
            for hit in resp["hits"]["hits"]:
                doc_id = hit["_id"]
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)
                source = hit["_source"]

                # 对网页添加快照
                if "snapshot_path" in source:
                    url_path = "/static/" +source["snapshot_path"].replace("\\", "/")

                recommended_results.append({
                    "title": source.get("title", "无标题"),
                    "url": source.get("url", "#"),
                    "score": hit["_score"],
                    "snapshot": url_path,
                    "type": source.get("doc_type", "web")
                })

        return recommended_results[:top_k]

    except Exception as e:
        print("推荐失败：", e)
        return []