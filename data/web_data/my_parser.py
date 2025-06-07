import os
import json
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
# 解析网页内容
def load_url_map(url_map_file):
    try:
        with open(url_map_file, 'r', encoding='utf-8') as f:
            raw_map = json.load(f)
            return {
                os.path.splitext(os.path.basename(k.replace("\\", "/")))[0]: v
                for k, v in raw_map.items()
            }
            #return json.load(f)
    except Exception as e:
        print(f"加载URL映射失败: {e}")
        return {}

def extract_text_from_html(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            title = soup.title.text.strip() if soup.title else ""
            body = soup.get_text(separator=' ', strip=True)
            links = [
                {"text": a.get_text(strip=True), "href": a.get('href', '')}
                for a in soup.find_all('a') 
                if a.get_text(strip=True)
            ]
            return title, body, links
    except Exception as e:
        print(f"解析 {file_path} 失败: {e}")
        return "", "", []

'''def parse_all_html(data_dir, url_map_file, output_jsonl):
    url_map = load_url_map(url_map_file)
    total = 0
    
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for filename in os.listdir(data_dir):
            if not filename.endswith('.html'):
                continue
            doc_id = os.path.splitext(filename)[0]
            file_path = os.path.join(data_dir, filename)
            title, body, links = extract_text_from_html(file_path)
            url = url_map.get(doc_id, "")
            
            doc = {
                "id": doc_id,
                "url": url,
                "title": title,
                "content": body,
                "anchors": links,
                "snapshot_path": file_path
            }
            
            json.dump(doc, f, ensure_ascii=False)
            f.write('\n')
            total += 1

    print(f"成功解析 {total} 篇文档至 {output_jsonl}")'''

def process_file(filename, data_dir, url_map):
    if not filename.endswith('.html'):
        return None
    doc_id = os.path.splitext(filename)[0]
    file_path = os.path.join(data_dir, filename)
    title, body, links = extract_text_from_html(file_path)
    return {
        "id": doc_id,
        "url": url_map.get(doc_id, ""),
        "title": title,
        "content": body,
        "anchors": links,
        "snapshot_path": file_path
    }
# 解析所有网页
def parse_all_html(data_dir, url_map_file, output_jsonl):
    url_map = load_url_map(url_map_file)
    files = [f for f in os.listdir(data_dir) if f.endswith('.html')]
    
    with ThreadPoolExecutor(max_workers=8) as executor, \
         open(output_jsonl, 'w', encoding='utf-8') as f:
        futures = [
            executor.submit(process_file, f, data_dir, url_map)
            for f in files
        ]
        for future in concurrent.futures.as_completed(futures):
            if (doc := future.result()) is not None:
                json.dump(doc, f, ensure_ascii=False)
                f.write('\n')
    
    print(f"成功解析 {len(files)} 篇文档至 {output_jsonl}")



if __name__ == "__main__":   
    data_list = [
        # 学院数据
        {"data_dir": "college_data/ai_data", "url_map": "url_map/url_map_ai.json", "output": "parsed_data/ai_parsed.jsonl"},
        {"data_dir": "college_data/bs_data", "url_map": "url_map/url_map_bs.json", "output": "parsed_data/bs_parsed.jsonl"},
        {"data_dir": "college_data/cc_data", "url_map": "url_map/url_map_cc.json", "output": "parsed_data/cc_parsed.jsonl"},
        {"data_dir": "college_data/ceo_data", "url_map": "url_map/url_map_ceo.json", "output": "parsed_data/ceo_parsed.jsonl"},
        {"data_dir": "college_data/chem_data", "url_map": "url_map/url_map_chem.json", "output": "parsed_data/chem_parsed.jsonl"},
        {"data_dir": "college_data/cs_data", "url_map": "url_map/url_map_cs.json", "output": "parsed_data/cs_parsed.jsonl"},
        {"data_dir": "college_data/cyber_data", "url_map": "url_map/url_map_cyber.json", "output": "parsed_data/cyber_parsed.jsonl"},
        {"data_dir": "college_data/cz_data", "url_map": "url_map/url_map_cz.json", "output": "parsed_data/cz_parsed.jsonl"},
        {"data_dir": "college_data/economics_data", "url_map": "url_map/url_map_economics.json", "output": "parsed_data/economics_parsed.jsonl"},
        {"data_dir": "college_data/env_data", "url_map": "url_map/url_map_env.json", "output": "parsed_data/env_parsed.jsonl"},
        {"data_dir": "college_data/finance_data", "url_map": "url_map/url_map_finance.json", "output": "parsed_data/finance_parsed.jsonl"},
        {"data_dir": "college_data/history_data", "url_map": "url_map/url_map_history.json", "output": "parsed_data/history_parsed.jsonl"},
        {"data_dir": "college_data/hyxy_data", "url_map": "url_map/url_map_hyxy.json", "output": "parsed_data/hyxy_parsed.jsonl"},
        {"data_dir": "college_data/jc_data", "url_map": "url_map/url_map_jc.json", "output": "parsed_data/jc_parsed.jsonl"},
        {"data_dir": "college_data/law_data", "url_map": "url_map/url_map_law.json", "output": "parsed_data/law_parsed.jsonl"},
        {"data_dir": "college_data/math_data", "url_map": "url_map/url_map_math.json", "output": "parsed_data/math_parsed.jsonl"},
        {"data_dir": "college_data/medical_data", "url_map": "url_map/url_map_medical.json", "output": "parsed_data/medical_parsed.jsonl"},
        {"data_dir": "college_data/mse_data", "url_map": "url_map/url_map_mse.json", "output": "parsed_data/mse_parsed.jsonl"},
        {"data_dir": "college_data/pharmacy_data", "url_map": "url_map/url_map_pharmacy.json", "output": "parsed_data/pharmacy_parsed.jsonl"},
        {"data_dir": "college_data/phil_data", "url_map": "url_map/url_map_phil.json", "output": "parsed_data/phil_parsed.jsonl"},
        {"data_dir": "college_data/physics_data", "url_map": "url_map/url_map_physics.json", "output": "parsed_data/physics_parsed.jsonl"},
        {"data_dir": "college_data/sfs_data", "url_map": "url_map/url_map_sfs.json", "output": "parsed_data/sfs_parsed.jsonl"},
        {"data_dir": "college_data/shxy_data", "url_map": "url_map/url_map_shxy.json", "output": "parsed_data/shxy_parsed.jsonl"},
        {"data_dir": "college_data/sky_data", "url_map": "url_map/url_map_sky.json", "output": "parsed_data/sky_parsed.jsonl"},
        {"data_dir": "college_data/stat_data", "url_map": "url_map/url_map_stat.json", "output": "parsed_data/stat_parsed.jsonl"},
        {"data_dir": "college_data/tas_data", "url_map": "url_map/url_map_tas.json", "output": "parsed_data/tas_parsed.jsonl"},
        {"data_dir": "college_data/wxy_data", "url_map": "url_map/url_map_wxy.json", "output": "parsed_data/wxy_parsed.jsonl"},
        {"data_dir": "college_data/zfxy_data", "url_map": "url_map/url_map_zfxy.json", "output": "parsed_data/zfxy_parsed.jsonl"},
        
        # 新闻数据
        {"data_dir": "news_data/news_gynk_data", "url_map": "url_map/url_map_gynk.json", "output": "parsed_data/news_gynk_parsed.jsonl"},
        {"data_dir": "news_data/news_mtnk_data", "url_map": "url_map/url_map_mtnk.json", "output": "parsed_data/news_mtnk_parsed.jsonl"},
        {"data_dir": "news_data/news_nkdxb_data", "url_map": "url_map/url_map_nkdxb.json", "output": "parsed_data/news_nkdxb_parsed.jsonl"},
        {"data_dir": "news_data/news_nkrw_data", "url_map": "url_map/url_map_nkrw.json", "output": "parsed_data/news_nkrw_parsed.jsonl"},
        {"data_dir": "news_data/news_xs_data", "url_map": "url_map/url_map_xs.json", "output": "parsed_data/news_xs_parsed.jsonl"},
        {"data_dir": "news_data/news_ywsd_data", "url_map": "url_map/url_map_ywsd.json", "output": "parsed_data/news_ywsd_parsed.jsonl"}
    ]

    os.makedirs("parsed_data", exist_ok=True)
    os.makedirs("url_map", exist_ok=True)

    for data in data_list:
        parse_all_html(
            data_dir=data["data_dir"],
            url_map_file=data["url_map"],
            output_jsonl=data["output"]
        )