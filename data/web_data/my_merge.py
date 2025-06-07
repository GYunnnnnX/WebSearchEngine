import glob
import json

#将jsonl文件合并在一起，为了后续导入索引
output_path = 'parsed_all.jsonl'
input_paths = [
    "parsed_data/ai_parsed.jsonl",
    "parsed_data/bs_parsed.jsonl",
    "parsed_data/cc_parsed.jsonl",
    "parsed_data/ceo_parsed.jsonl",
    "parsed_data/chem_parsed.jsonl",
    "parsed_data/cs_parsed.jsonl",
    "parsed_data/cyber_parsed.jsonl",
    "parsed_data/cz_parsed.jsonl",
    "parsed_data/economics_parsed.jsonl",
    "parsed_data/env_parsed.jsonl",
    "parsed_data/finance_parsed.jsonl",
    "parsed_data/history_parsed.jsonl",
    "parsed_data/hyxy_parsed.jsonl",
    "parsed_data/jc_parsed.jsonl",
    "parsed_data/law_parsed.jsonl",
    "parsed_data/math_parsed.jsonl",
    "parsed_data/medical_parsed.jsonl",
    "parsed_data/mse_parsed.jsonl",
    "parsed_data/news_gynk_parsed.jsonl",
    "parsed_data/news_mtnk_parsed.jsonl",
    "parsed_data/news_nkdxb_parsed.jsonl",
    "parsed_data/news_nkrw_parsed.jsonl",
    "parsed_data/news_xs_parsed.jsonl",
    "parsed_data/news_ywsd_parsed.jsonl",
    "parsed_data/pharmacy_parsed.jsonl",
    "parsed_data/phil_parsed.jsonl",
    "parsed_data/physics_parsed.jsonl",
    "parsed_data/sfs_parsed.jsonl",
    "parsed_data/shxy_parsed.jsonl",
    "parsed_data/sky_parsed.jsonl",
    "parsed_data/stat_parsed.jsonl",
    "parsed_data/tas_parsed.jsonl",
    "parsed_data/wxy_parsed.jsonl",
    "parsed_data/zfxy_parsed.jsonl"
]

with open(output_path, 'w', encoding='utf-8') as outfile:
    for path in input_paths:
        try:
            with open(path, 'r', encoding='utf-8') as infile:
                for line in infile:
                     # 核心修改：替换异常行终止符为空字符串
                    line = line.replace('\u2028', '').replace('\u2029', '')       # 移除段落分隔符
                    outfile.write(line)
            print(f"已合并: {path}")
        except FileNotFoundError:
            print(f"警告: 文件 {path} 不存在，已跳过")
        except Exception as e:
            print(f"处理文件 {path} 时出错: {str(e)}")

print(f"合并完成，结果保存在 {output_path}")