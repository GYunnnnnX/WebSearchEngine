import os
import json
from glob import glob

#将json文件合并在一起，方便后面导入索引
def merge_to_jsonl(input_dir, output_path):
    """
    合并目录下所有 url_map*.json 文件，转换为 JSONL 格式。
    
    Args:
        input_dir (str): 包含 url_map*.json 文件的目录
        output_path (str): 合并后的 JSONL 输出路径
    """
    pattern = os.path.join(input_dir, '**', 'url_map*.json')
    all_files = glob(pattern, recursive=True)

    with open(output_path, 'w', encoding='utf-8') as out_file:
        for file_path in all_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for doc_path, meta in data.items():
                        item = {
                            "id": doc_path,
                            "url": meta.get("url", ""),
                            "title": meta.get("title", ""),
                            "original_filename": meta.get("original_filename", "")
                        }
                        out_file.write(json.dumps(item, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f"读取失败 {file_path}：{e}")

    print(f"已生成 JSONL 文件：{output_path}")

if __name__ == "__main__":
    merge_to_jsonl("document_url_map", "document_all.jsonl")
