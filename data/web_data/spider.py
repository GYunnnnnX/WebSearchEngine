import requests
from bs4 import BeautifulSoup
import os
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置参数

'''# 光影南开
BASE_DOMAIN = "news.nankai.edu.cn"
BASE_PATH = "/gynk/"  # 限制只爬取指定路径下的内容
PAGE_PREFIX = "https://news.nankai.edu.cn/gynk/system/count//0020000/000000000000/000/000/c0020000000000000000_"
SAVE_DIR = "news_data/news_gynk_data"
URL_MAP_FILE = "url_map/url_map_gynk.json"
pages = 12'''


'''# 南开要闻
BASE_DOMAIN = "news.nankai.edu.cn"
BASE_PATH = "/ywsd/"  # 限制只爬取指定路径下的内容
PAGE_PREFIX = "http://news.nankai.edu.cn/ywsd/system/count//0003000/000000000000/000/000/c0003000000000000000_"
SAVE_DIR = "news_data/news_ywsd_data"
URL_MAP_FILE = "url_map/url_map_ywsd.json"
pages = 655'''

'''# 南开故事
BASE_DOMAIN = "news.nankai.edu.cn"
BASE_PATH = "/nkrw/"  # 限制只爬取指定路径下的内容
PAGE_PREFIX = "http://news.nankai.edu.cn/nkrw/system/count//0008000/000000000000/000/000/c0008000000000000000_"
SAVE_DIR = "news_data/news_nkrw_data"
URL_MAP_FILE = "url_map/url_map_nkrw.json"
pages = 68'''

'''# 南开大学报
BASE_DOMAIN = "news.nankai.edu.cn"
BASE_PATH = "/nkdxb/"  # 限制只爬取指定路径下的内容
PAGE_PREFIX = "http://news.nankai.edu.cn/nkdxb/system/count//0011000/000000000000/000/000/c0011000000000000000_"
SAVE_DIR = "news_data/news_nkdxb_data"
URL_MAP_FILE = "url_map/url_map_nkdxb.json"
pages = 78'''

# 媒体南开
BASE_DOMAIN = "news.nankai.edu.cn"
BASE_PATH = "/mtnk/" 
PAGE_PREFIX = "http://news.nankai.edu.cn/mtnk/system/count//0006000/000000000000/000/000/c0006000000000000000_"
SAVE_DIR = "news_data/news_mtnk_data"
URL_MAP_FILE = "url_map/url_map_mtnk.json"
pages = 999

MAX_PAGES = 100000 
REQUEST_INTERVAL = 0.3  #请求间隔
#USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(URL_MAP_FILE), exist_ok=True)  

def load_existing_map():
    """加载已有的URL映射"""
    if os.path.exists(URL_MAP_FILE):
        with open(URL_MAP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def fetch_url(url):
    #headers = {"User-Agent": USER_AGENT}
    try:
        #response = requests.get(url, headers=headers, timeout=10)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        #验证内容类型
        content_type = response.headers.get("Content-Type", "").split(";")[0]
        if content_type != "text/html":
            print(f"跳过非HTML内容: {url} ({content_type})")
            return None
            
        response.encoding = response.apparent_encoding
        return response.text
    except requests.RequestException as e:
        print(f"请求失败: {url} - {str(e)}")
        return None

def normalize_url(url):
    # 标准化
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

def parse_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    
    for tag in soup.find_all("a", href=True):
        raw_url = tag["href"].strip()
        if not raw_url or raw_url.startswith("javascript:"):
            continue
            
        try:
            full_url = urljoin(base_url, raw_url)
            parsed = urlparse(full_url)
            
            # 域名和路径过滤
            if parsed.netloc != BASE_DOMAIN:
                continue
            if not parsed.path.startswith(BASE_PATH):
                continue

            clean_url = normalize_url(full_url)
            links.add(clean_url)
        except ValueError:
            continue
            
    return links

def get_page_filename(url):
    #生成基于哈希的唯一文件名
    return os.path.join(SAVE_DIR, f"{hashlib.md5(url.encode()).hexdigest()}.html")

def generate_page_urls():
    return [f"{PAGE_PREFIX}{i:09d}.shtml" for i in range(1, pages+1)]



def save_data(url_map):
    try:
        with open(URL_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(url_map, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存失败: {e}")
        raise


def crawl():
    url_map = load_existing_map()
    visited = set(url_map.values())
    queue = [url for url in generate_page_urls() if url not in visited]
    queued = set(queue) 

    save_interval = 100
    page_count = 0

    while queue and len(url_map) < MAX_PAGES:
        current_url = queue.pop(0)
        queued.discard(current_url)  # 移除当前已处理的
        print(f"正在爬取 ({len(url_map)}/{MAX_PAGES}): {current_url}")

        html = fetch_url(current_url)
        if not html:
            continue

        filename = get_page_filename(current_url)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        url_map[filename] = current_url
        visited.add(current_url)

        new_links = parse_links(html, current_url)
        for link in new_links:
            if link not in visited and link not in queued:
                queue.append(link)
                queued.add(link)  # 入队时也记录

        if len(queued) > 10_000:
            queued = set(u for u in queued if u not in visited)

        page_count += 1
        if page_count % save_interval == 0:
            save_data(url_map)
            print(f"已保存进度（当前{len(url_map)}页）")

        time.sleep(REQUEST_INTERVAL)

    save_data(url_map)
    print(f"爬取完成，共抓取 {len(url_map)} 个页面")


def crawl_parallel():
    url_map = load_existing_map()
    visited = set(url_map.values())
    queue = [url for url in generate_page_urls() if url not in visited]
    queued = set(queue)
    save_interval = 100
    page_count = 0

    def process_url(current_url):
        html = fetch_url(current_url)
        if not html:
            return None

        filename = get_page_filename(current_url)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        
        new_links = parse_links(html, current_url)
        return current_url, filename, new_links

    with ThreadPoolExecutor(max_workers=10) as executor:  # 线程数10
        while queue and len(url_map) < MAX_PAGES:
            current_url = queue.pop(0)
            queued.discard(current_url)
            
            future = executor.submit(process_url, current_url)
            try:
                result = future.result(timeout=15) 
                if not result:
                    continue

                current_url, filename, new_links = result
                url_map[filename] = current_url
                visited.add(current_url)

                for link in new_links:
                    if link not in visited and link not in queued:
                        queue.append(link)
                        queued.add(link)

                page_count += 1
                if page_count % save_interval == 0:
                    save_data(url_map)
                    print(f"进度: {len(url_map)}/{MAX_PAGES}")

            except Exception as e:
                print(f"处理失败: {current_url} - {e}")
                queue.append(current_url)  #失败后重新加入队列

    save_data(url_map)
    print(f"完成，共爬取 {len(url_map)} 页")


if __name__ == "__main__":
    # for url in generate_page_urls():
    #     print(f"{url} => {'有效' if fetch_url(url) else '无效'}")
    
    crawl_parallel()
    print("数据映射保存至:", URL_MAP_FILE)