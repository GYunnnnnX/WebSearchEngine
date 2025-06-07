# elastic_utils.py
from elasticsearch import Elasticsearch
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone

# 注册
def create_user(es: Elasticsearch, username, password, email):
    doc = {
        "username": username,
        "password_hash": generate_password_hash(password),
        "email": email,
        "preferences": {
            "favorite_categories": [],
            "search_history": []
        },
        "created_at": datetime.now(timezone.utc) 
    }
    es.index(index="users_indices", id=username, document=doc)


# 验证用户，用户名+密码
def authenticate_user(es: Elasticsearch, username, password):
    try:
        res = es.get(index="users_indices", id=username)
        return check_password_hash(res["_source"]["password_hash"], password)
    except:
        return False

# 记录搜索日志
def log_query(es: Elasticsearch, username, query_text, search_type, site_filter, num_results):
    # 写入搜索日志索引
    es.index(index="search_query_log", document={
        "timestamp": datetime.now(timezone.utc),
        "query_text": query_text,
        "search_type": search_type,
        "site_filter": site_filter,
        "num_results": num_results,
        "username": username
    })

    # 更新用户索引中的搜索历史
    try:
        es.update(
            index="users_indices",
            id=username,
            body={
                "script": {
                    "source": """
                        if (ctx._source.preferences.search_history == null) {
                            ctx._source.preferences.search_history = [params.query];
                        } else {
                            ctx._source.preferences.search_history.add(params.query);
                        }
                    """,
                    "lang": "painless",
                    "params": {
                        "query": query_text
                    }
                }
            }
        )
    except Exception as e:
        print("[log_query] Failed to update user search history:", e)

# 获取历史搜索记录
def get_user_history(es: Elasticsearch, username):
    try:
        res = es.get(index="users_indices", id=username)
        return res["_source"].get("preferences", {}).get("search_history", [])
    except:
        return []
