# search_logic.py
from elasticsearch import Elasticsearch
import hashlib
import elastic_utils
import re

def clean_text(text):
    """去掉多余空格、换行，统一格式"""
    return re.sub(r"\s+", " ", text or "").strip().lower()


# 通配查询构建函数
def build_wildcard_query(query_text):
    if '*' not in query_text:
        query_text += '*'  #自动补全通配符，前后各有一个
        query_text = '*'+ query_text

    return {
        "wildcard": {
            "title.wildcard": {
                "value": query_text.lower(),
                "case_insensitive": True
            }
        }
    }

# 个性化查询：获取用户的搜索历史，根据历史搜索词频，推荐相关搜索词
def get_user_preferences(es: Elasticsearch, user: str, top_k=5):
    try:
        user_doc = es.get(index="users_indices", id=user)
        history = elastic_utils.get_user_history(es, user)
        from collections import Counter
        counter = Counter(history)
        return [kw for kw, _ in counter.most_common(top_k)]
    except:
        return []
    
# 搜索函数
def advanced_search(es: Elasticsearch, index_name, query_text, top_k=20, 
                   site_filter=None, exact_phrase=False, wildcard_query=False,user=None):
    query = {"bool": {"must": []}}

    filters = []
    if site_filter:
        filters.append({"wildcard": {"url": f"*{site_filter}*"}})

    if filters:
        query["bool"]["filter"] = filters

    if exact_phrase:
        query["bool"]["must"].append({
            "match_phrase": {
                "content": {
                    "query": query_text,
                    "slop": 3
                }
            }
        })

    elif wildcard_query:
        wildcard_query = build_wildcard_query(query_text)
        query["bool"]["must"].append(wildcard_query)

    else:
        fields = ["title"]
        if index_name == "web_search_engine":
            fields += ["content", "anchors"]
        query["bool"]["must"].append({
            "multi_match": {
                "query": query_text,
                "fields": fields,
                "type": "best_fields"
            }
        })

    raw_response = es.search(index=index_name, body={"query": query}, size=top_k * 5)

    # 个性化关键词
    user_keywords = get_user_preferences(es, user) if user else []

    results = []
    # 有些文档url不同，但实际上是相同的内容，需要去重
    seen_hashes = set()  #存放去重用的哈希值

    for hit in raw_response["hits"]["hits"]:
        source = hit["_source"]

        # title+content去重
        title = source.get("title", "")
        content = source.get("content", "") if index_name == "web_search_engine" else ""
        check = content[20:25]
        hash_input = (title + check).strip().encode("utf-8")
        doc_hash = hashlib.sha256(hash_input).hexdigest()

        if doc_hash in seen_hashes:
            continue   
        seen_hashes.add(doc_hash)

        final_score = hit["_score"]

        # 加权用户偏好关键词
        for kw in user_keywords:
            title_count = title.lower().count(kw.lower())
            content_count = content.lower().count(kw.lower())

            # 基础分+次数加权
            if title_count > 0:
                final_score += 3.5 + min(title_count * 0.5, 5.0)  # 标题每次出现加0.5，最多加5
            if content_count > 0:
                final_score += 2.0 + min(content_count * 0.3, 3.0)  # 内容每次出现加0.3，最多加3

        result = {
            "id": source.get("id", "N/A"),
            "title": source.get("title", "N/A"),
            "url": source.get("url", "N/A"),
            "score": final_score,
            "type": source.get("doc_type", "unknown")
        }
        # 对网页添加快照
        if "snapshot_path" in source:
            url_path = "/static/" +source["snapshot_path"].replace("\\", "/")
            result["snapshot"] = url_path
        
        results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]
    
    
    '''elif wildcard_query:
        query["bool"]["must"].append({
            "wildcard": {
                "title": {
                    "value": f"*{query_text}*",
                    "case_insensitive": True
                }
            }
        })'''