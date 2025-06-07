# search.py
from elasticsearch import Elasticsearch
import argparse
import json
from datetime import datetime

# !!!!!此函数弃用，搜索逻辑在webapp/search_logic.py中


'''#添加查询记录
def add_search_history(es, user_id, query_text, search_type, 
                     site_filter=None, doc_type=None):
    history_entry = {
        "query": query_text,
        "timestamp": datetime.utcnow(),
        "type": search_type,
        "filters": {}
    }
    
    if site_filter:
        history_entry["filters"]["site"] = site_filter
    if doc_type:
        history_entry["filters"]["doc_type"] = doc_type
    
    es.update(
        index="users_indices",
        id=user_id,
        body={
            "script": {
                "source": """
                    if (ctx._source.search_history == null) {
                        ctx._source.search_history = new ArrayList();
                    }
                    ctx._source.search_history.add(params.new_entry);
                    // 保留最近50条历史记录
                    if (ctx._source.search_history.length > 50) {
                        ctx._source.search_history.remove(0);
                    }
                """,
                "lang": "painless",
                "params": {"new_entry": history_entry}
            }
        }
    )

# 查询功能
def search(es, index_name, query_text, user_id=None,top_k=10, 
                   site_filter=None, exact_phrase=False, 
                   wildcard_query=False, doc_type=None):
    # 构建查询主体
    query = {"bool": {"must": []}}
    
    # 站内查询
    if site_filter:
        query["bool"]["filter"] = {"wildcard": {"url": f"*{site_filter}*"}}
    
    # 文档类型过滤
    if doc_type:
        query["bool"]["filter"] = {"term": {"doc_type": doc_type}}
    
    # 短语查询
    if exact_phrase:
        query["bool"]["must"].append({
            "match_phrase": {
                "content": {
                    "query": query_text,
                    "slop": 3  # 允许少量词语间隔
                }
            }
        })
    # 4. 通配查询
    elif wildcard_query:
        query["bool"]["must"].append({
            "wildcard": {
                "title": {
                    "value": f"*{query_text}*",
                    "case_insensitive": True
                }
            }
        })
    # 5. 默认多字段匹配查询
    else:
        fields = ["title", "content"]
        if index_name == "web_search_engine":
            fields.append("anchors")
        
        query["bool"]["must"].append({
            "multi_match": {
                "query": query_text,
                "fields": fields,
                "type": "best_fields"
            }
        })
    

    # 执行搜索
    response = es.search(
        index=index_name,
        body={"query": query},
        size=top_k
    )
    
    # 处理结果
    results = []
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        result = {
            "id": source.get("id", "N/A"),
            "title": source.get("title", "N/A"),
            "url": source.get("url", "N/A"),
            "score": hit["_score"],
            "type": source.get("doc_type", "unknown")
        }
        
        # 添加网页快照信息
        if "snapshot_path" in source:
            result["snapshot"] = source["snapshot_path"]
        
        results.append(result)
    
    return results
'''