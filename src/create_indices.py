from elasticsearch import Elasticsearch

# 使用 Elasticsearch 的 HTTP API 创建索引：
# web_search_engine
# web_search_engine_document
# users_indices 
'''# 创建网页索引
def create_web_index(es):
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "url": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
                "anchors": {"type": "text"},
                "snapshot_path": {"type": "keyword"},
                "doc_type": {"type": "keyword"}
            }
        }
    }
   # es.indices.create(index="web_search_engine", body=mapping, ignore=400)
    es.options(ignore_status=400).indices.create(index="web_search_engine", mappings=mapping)


# 创建文档索引
def create_document_index(es):
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "url": {"type": "keyword"},
                "title": {"type": "text"},
                "original_filename": {"type": "keyword"},
                "doc_type": {"type": "keyword"}
            }
        }
    }
   # es.indices.create(index="web_search_engine_document", body=mapping, ignore=400)
    es.options(ignore_status=400).indices.create(index="web_search_engine_document", mappings=mapping)'''

def create_web_index(es):
    settings = {
        "analysis": {
            "analyzer": {
                "wildcard_analyzer": {
                    "tokenizer": "keyword", #分词
                    "filter": ["lowercase"]  #统一小写
                }
            }
        }
    }
    
    mappings = {
        #"settings": settings,
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "url": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "ik_max_word",  #ik_max_word插件中文分词
                    "fields": {
                        "wildcard": {  #通配查询专用子字段
                            "type": "text",
                            "analyzer": "wildcard_analyzer"
                        }
                    }
                },
                "content": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "fields": {
                        "wildcard": {
                            "type": "text",
                            "analyzer": "wildcard_analyzer"
                        }
                    }
                },
                #"anchors": {"type": "text"},
                "anchors": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "text"},
                        "href": {"type": "keyword"}
                    }
                },
                "snapshot_path": {"type": "keyword"},
                "doc_type": {"type": "keyword"}
            }
        }
    }

    body = {
    "settings": settings,
    "mappings": mappings["mappings"]
    }
    es.options(ignore_status=400).indices.create(index="web_search_engine", body=body)

def create_document_index(es):
    settings = {
        "analysis": {
            "analyzer": {
                "wildcard_analyzer": {
                    "tokenizer": "keyword",
                    "filter": ["lowercase"]
                }
            }
        }
    }
    
    mappings = {
        #"settings": settings,
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "url": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "fields": {
                        "wildcard": {
                            "type": "text",
                            "analyzer": "wildcard_analyzer"
                        }
                    }
                },
                "original_filename": {"type": "keyword"},
                "doc_type": {"type": "keyword"}
            }
        }
    }
    body = {
    "settings": settings,
    "mappings": mappings["mappings"]
    }
    es.options(ignore_status=400).indices.create(index="web_search_engine_document", body=body)


# 创建用户索引
def create_user_index(es):
    mapping = {
        "mappings": {
            "properties": {
                "username": {"type": "keyword"},
                "password_hash": {"type": "keyword"},
                "email": {"type": "keyword"},
                "preferences": {
                    "type": "object",
                    "properties": {
                        "favorite_categories": {"type": "keyword"},
                    }
                },
                #完整的搜索历史
                "search_history": {
                    "type": "nested",  
                    "properties": {
                        "query": {"type": "text"},  #搜索关键词
                        "timestamp": {"type": "date"}, #时间
                        "type": {"type": "keyword"},  # 搜索类型web/doc
                        "filters": {  # 搜索过滤条件
                            "type": "object",
                            "properties": {
                                "site": {"type": "keyword"},
                                "doc_type": {"type": "keyword"}
                            }
                        }
                    }
                },
                "created_at": {"type": "date"}
            }
        }
    }
    es.options(ignore_status=400).indices.create(index="users_indices", mappings=mapping)

# 可以用来删除索引
def delete_indices(es):
    es.indices.delete(index="web_search_engine", ignore_unavailable=True)
    es.indices.delete(index="web_search_engine_document", ignore_unavailable=True)

if __name__ == "__main__":
    #es = Elasticsearch("https://localhost:9200")  
    es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "=j2jb70ynctOB8fIaOdi"), 
    verify_certs=False  # 跳过证书验证（相当于 curl -k）
    )
    #delete_indices(es)
    # 创建网页索引
    create_web_index(es)
    # 创建文档索引
    create_document_index(es)
    # 创建查询日志索引
    create_user_index(es)