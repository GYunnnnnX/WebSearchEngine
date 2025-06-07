import requests
from bs4 import BeautifulSoup
import os
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
# 不同学院的配置
'''
# 校史
BASE_URL = "https://xs.nankai.edu.cn"
SAVE_DIR = "news_data/news_xs_data"
URL_MAP_FILE = "url_map/url_map_xs.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 计算机学院
BASE_URL = "https://cc.nankai.edu.cn"
SAVE_DIR = "college_data/cc_data"
URL_MAP_FILE = "url_map/url_map_cc.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 金融学院
BASE_URL = "https://finance.nankai.edu.cn/"
SAVE_DIR = "college_data/finance_data"
URL_MAP_FILE = "url_map/url_map_finance.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 文学院
BASE_URL = "https://wxy.nankai.edu.cn/"
SAVE_DIR = "college_data/wxy_data"
URL_MAP_FILE = "url_map/url_map_wxy.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 哲学院
BASE_URL = "https://phil.nankai.edu.cn/"
SAVE_DIR = "college_data/phil_data"
URL_MAP_FILE = "url_map/url_map_phil.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 周恩来政府管理学院
BASE_URL = "https://zfxy.nankai.edu.cn/"
SAVE_DIR = "college_data/zfxy_data"
URL_MAP_FILE = "url_map/url_map_zfxy.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 马克思主义学院
BASE_URL = "https://cz.nankai.edu.cn/"
SAVE_DIR = "college_data/cz_data"
URL_MAP_FILE = "url_map/url_map_cz.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 商学院
BASE_URL = "https://bs.nankai.edu.cn/"
SAVE_DIR = "college_data/bs_data"
URL_MAP_FILE = "url_map/url_map_bs.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 数学科学学院
BASE_URL = "https://math.nankai.edu.cn/"
SAVE_DIR = "college_data/math_data"
URL_MAP_FILE = "url_map/url_map_math.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 化学学院
BASE_URL = "https://chem.nankai.edu.cn/"
SAVE_DIR = "college_data/chem_data"
URL_MAP_FILE = "url_map/url_map_chem.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 医学院
BASE_URL = "https://medical.nankai.edu.cn/"
SAVE_DIR = "college_data/medical_data"
URL_MAP_FILE = "url_map/url_map_medical.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 人工智能学院
BASE_URL = "https://ai.nankai.edu.cn/"
SAVE_DIR = "college_data/ai_data"
URL_MAP_FILE = "url_map/url_map_ai.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 电子信息与光学工程学院
BASE_URL = "https://ceo.nankai.edu.cn/"
SAVE_DIR = "college_data/ceo_data"
URL_MAP_FILE = "url_map/url_map_ceo.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 汉语言文化学院
BASE_URL = "https://hyxy.nankai.edu.cn/"
SAVE_DIR = "college_data/hyxy_data"
URL_MAP_FILE = "url_map/url_map_hyxy.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 药学院
BASE_URL = "https://pharmacy.nankai.edu.cn/"
SAVE_DIR = "college_data/pharmacy_data"
URL_MAP_FILE = "url_map/url_map_pharmacy.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 材料科学与工程学院
BASE_URL = "https://mse.nankai.edu.cn/"
SAVE_DIR = "college_data/mse_data"
URL_MAP_FILE = "url_map/url_map_mse.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 社会学院
BASE_URL = "https://shxy.nankai.edu.cn/"
SAVE_DIR = "college_data/shxy_data"
URL_MAP_FILE = "url_map/url_map_shxy.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 历史学院
BASE_URL = "https://history.nankai.edu.cn/"
SAVE_DIR = "college_data/history_data"
URL_MAP_FILE = "url_map/url_map_history.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 法学院
BASE_URL = "https://law.nankai.edu.cn/"
SAVE_DIR = "college_data/law_data"
URL_MAP_FILE = "url_map/url_map_law.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 外国语学院
BASE_URL = "https://sfs.nankai.edu.cn/"
SAVE_DIR = "college_data/sfs_data"
URL_MAP_FILE = "url_map/url_map_sfs.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 经济学院
BASE_URL = "https://economics.nankai.edu.cn/"
SAVE_DIR = "college_data/economics_data"
URL_MAP_FILE = "url_map/url_map_economics.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 统计与数据科学学院
BASE_URL = "https://stat.nankai.edu.cn/"
SAVE_DIR = "college_data/stat_data"
URL_MAP_FILE = "url_map/url_map_stat.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 物理科学学院
BASE_URL = "https://physics.nankai.edu.cn/"
SAVE_DIR = "college_data/physics_data"
URL_MAP_FILE = "url_map/url_map_physics.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 生命科学学院
BASE_URL = "https://sky.nankai.edu.cn/"
SAVE_DIR = "college_data/sky_data"
URL_MAP_FILE = "url_map/url_map_sky.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 网络空间安全学院
BASE_URL = "https://cyber.nankai.edu.cn/"
SAVE_DIR = "college_data/cyber_data"
URL_MAP_FILE = "url_map/url_map_cyber.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 软件学院
BASE_URL = "https://cs.nankai.edu.cn/"
SAVE_DIR = "college_data/cs_data"
URL_MAP_FILE = "url_map/url_map_cs.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 旅游与服务学院
BASE_URL = "https://tas.nankai.edu.cn/"
SAVE_DIR = "college_data/tas_data"
URL_MAP_FILE = "url_map/url_map_tas.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

'''
# 新闻传播学院
BASE_URL = "https://jc.nankai.edu.cn/"
SAVE_DIR = "college_data/jc_data"
URL_MAP_FILE = "url_map/url_map_jc.json"
MAX_PAGES = 100000
NUM_THREADS = 10
'''

# 环境科学与工程学院
BASE_URL = "https://env.nankai.edu.cn/"
SAVE_DIR = "college_data/env_data"
URL_MAP_FILE = "url_map/url_map_env.json"
MAX_PAGES = 100000
NUM_THREADS = 10

os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(URL_MAP_FILE), exist_ok=True)

visited = set()
visited_lock = threading.Lock()

url_map = {}
url_map_lock = threading.Lock()

page_count = 0
page_count_lock = threading.Lock()

url_queue = Queue()
url_queue.put(BASE_URL)

def fetch_url(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if "text/html" not in response.headers.get("Content-Type", ""):
            print(f"Non-HTML content at {url}")
            return None
        response.encoding = response.apparent_encoding
        return response.text
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return None

def parse_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag['href']
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)
        if parsed_url.netloc == urlparse(base_url).netloc:
            links.add(full_url.split("#")[0])
    return links

def save_page(content, file_name):
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(content)

def get_page_filename(url):
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return os.path.join(SAVE_DIR, f"{url_hash}.html")

def worker():
    global page_count
    while True:
        if page_count >= MAX_PAGES:
            break

        try:
            url = url_queue.get(timeout=1)
        except:
            break

        with visited_lock:
            if url in visited:
                continue
            visited.add(url)

        print(f"[Thread-{threading.get_ident()}] Scraping: {url}")
        html = fetch_url(url)
        if not html:
            continue

        file_name = get_page_filename(url)
        save_page(html, file_name)

        with url_map_lock:
            url_map[file_name] = url

        with page_count_lock:
            page_count += 1
            if page_count >= MAX_PAGES:
                break

        new_links = parse_links(html, BASE_URL)
        with visited_lock:
            for link in new_links:
                if link not in visited:
                    url_queue.put(link)

        time.sleep(0.01)

# 核心并行爬取函数
def scrape_parallel():
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker) for _ in range(NUM_THREADS)]
        for f in as_completed(futures):
            pass

    with open(URL_MAP_FILE, "w", encoding="utf-8") as url_file:
        json.dump(url_map, url_file, indent=2, ensure_ascii=False)

    print(f"Scraped {page_count} pages.")
    print(f"URL mapping saved to {URL_MAP_FILE}")

if __name__ == "__main__":
    scrape_parallel()
