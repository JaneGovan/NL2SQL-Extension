from openai import OpenAI, AsyncOpenAI
from typing import Dict, List, Optional, Any
from traceback import print_exc
import re
import sqlite3
from concurrent.futures import as_completed, ProcessPoolExecutor
import asyncio
import json
import os
from json_repair import repair_json
from pprint import pprint
from glob import glob
from tqdm import tqdm
from pydantic import BaseModel, Field
from langchain.output_parsers import PydanticOutputParser
from dotenv import load_dotenv
load_dotenv()

class NlAndSQL(BaseModel):
    query: str = Field(description="自然语言。")
    sql: str = Field(description="该自然语言对应的SQL语句。")

def open_chat(prompt:str):
    client = OpenAI(api_key=os.getenv("API_KEY"), base_url=os.getenv("BASE_URL"))
    response = client.chat.completions.create(
        messages=[
            {"role":"user","content":prompt}
            ],
        model=os.getenv("MODEL_NAME"),
        temperature=0.6,
        top_p=0.9,
        max_tokens=2048,
        timeout=60
    )
    text = response.choices[0].message.content
    clean_text = text.replace('```json','').replace('```','').strip()
    return clean_text

async def a_open_chat(prompt:str):
    client = AsyncOpenAI(api_key=os.getenv("API_KEY"), base_url=os.getenv("BASE_URL"))
    response = await client.chat.completions.create(
        messages=[
            {"role":"user","content":prompt}
            ],
        model=os.getenv("MODEL_NAME"),
        temperature=0.6,
        top_p=0.9,
        max_tokens=2048,
        timeout=60
    )
    text = response.choices[0].message.content
    clean_text = text.replace('```json','').replace('```','').strip()
    return clean_text

def fix_table_name_in_sql(query_sql: Dict[str, Any], db_dir: str = "./data_example/train_databases"):
    new_query_sql = query_sql
    db_name = query_sql["db_id"]
    map_table_schema = Map_db_table_schema.get(db_name)
    sql = query_sql["query"]
    conn = sqlite3.connect(glob(f"{db_dir}/**/{db_name}.sqlite", recursive=True)[0])
    cursor = conn.cursor()
    gt_tables = list(map_table_schema.keys())
    for gtt in gt_tables:
        li_table_name = list(set(re.findall(fr"\b{gtt}\b", sql, re.IGNORECASE)))
        if li_table_name:
            for t_n in li_table_name:
                sql = re.sub(fr"\b{t_n}\b", gtt, sql)
        cursor.execute(f"PRAGMA table_info({gtt})")
        gt_columns = [i[1] for i in cursor.fetchall()]
        for gtc in gt_columns:
            li_columns = list(set(re.findall(fr"\b{gtc}\b", sql, re.IGNORECASE)))
            if li_columns:
                for c_n in li_columns:
                    sql = re.sub(fr"\b{c_n}\b", gtc, sql)
    new_query_sql["query"] = sql
    return new_query_sql

def repair_sql(query_sql: Dict[str, Any], db_dir: str = "./data_example/train_databases"):
    out_parser = PydanticOutputParser(pydantic_object=NlAndSQL)
    query_sql = fix_table_name_in_sql(query_sql, db_dir)
    related_table_schema = get_related_table_schema(query_sql)
    prompt = f"""
### 表结构 ###
{related_table_schema}

### 自然语言和SQL语句 ###
自然语言：{query_sql["question"]}
SQL语句：{query_sql["query"]}

### 任务 ###
根据自然语言的查询意图，如果SQL语句存在错误，则修正并优化。（严格遵循MySQL语法规则）

### 实现步骤 ###
Step 1: 判断### 自然语言和SQL语句 ###中的SQL语句是否存在错误。
Step 2: case 1: 如果存在错误，则修正并优化SQL语句。
        case 2: 如果没有错误，则直接返回### 自然语言和SQL语句 ###（不做修改）。

### 输出格式 ###
{out_parser.get_format_instructions()}
注意：必须按照### 输出格式 ###输出标准的JSON（包含输出格式中的所有的字段），不要输出任何多余的内容。
"""
    try:
        res = open_chat(prompt)
        response = out_parser.parse(repair_json(res)).model_dump()
    except Exception() as e:
        response = {
            "query": {query_sql["question"]},
            "sql": {query_sql["query"]}
        }
    query_sql["question"] = response["query"]
    query_sql["query"] = response["sql"]
    query_sql = fix_table_name_in_sql(query_sql, db_dir)
    return query_sql

def is_right_sql(query_sql: Dict[str, Any], db_dir: str = "./data_example/train_databases") -> bool:
    db_name = query_sql["db_id"]
    conn = sqlite3.connect(f"{glob(f'{db_dir}/**/{db_name}.sqlite')[0]}")
    cursor = conn.cursor()
    try:
        cursor.execute(f"{query_sql['query']}")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        if len(data) >= 1:
            return True
        else:
            return False
    except:
        return False

def get_map_all_table_schema_by_sqlite(data_path: str, out_path: str="./db_table_schema.json")->Dict[str, Any]:
    map_db = {}
    count_tables = 0
    sql_files = glob(os.path.join(data_path, f'**/*.sqlite'), recursive=True)
    for sql_f in tqdm(sql_files, desc="Extract all table schema."):
        db_name = os.path.splitext(os.path.basename(sql_f))[0]
        # print(db_name)
        map_table_schema = {}
        conn = sqlite3.connect(sql_f)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for table in tables:
            table_name_src = table[0]
            table_name = f"`{table_name_src}`"
            # print(table_name)
            str_table_schema = f"CREATE TABLE {table_name} ("
            if table_name.startswith('sqlite_'):
                continue
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            # pprint(columns)
            li_primary_key = []
            for column in columns:
                column_name = f"`{column[1]}`"
                type = column[2]
                not_null = column[3]
                default_value = column[4]
                if isinstance(default_value, str):
                    default_set = " DEFAULT "+default_value
                else:
                    default_set = " DEFAULT "+str(default_value)
                primary_key = column[5]
                if primary_key:
                    li_primary_key.append(column_name)
                str_table_schema += f"\n{column_name} {type}{' NOT NULL' if not_null else ''}{default_set if default_value else ''},"
            if li_primary_key:
                str_table_schema += "\nPRIMARY KEY ("
                for idx, p_k in enumerate(li_primary_key):
                    if idx == 0:
                        str_table_schema += p_k
                    else:
                        str_table_schema += f",{p_k}"
                str_table_schema += "),"
            # pprint(str_table_schema)
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            pre_foreign_keys = cursor.fetchall()
            
            if pre_foreign_keys:
                foreign_keys = sorted(pre_foreign_keys)
                # pprint(foreign_keys)
                str_foreign_key = ""
                rel_keys = []
                reled_keys = []
                before_id = -1
                before_reled_table_name = ""
                before_restrict_update = ""
                before_restrict_delete = ""
                before_match = ""
                for idx, foreign_key in enumerate(foreign_keys):
                    reled_table_name = foreign_key[2]
                    restrict_update =  foreign_key[5].upper()
                    restrict_delete =  foreign_key[6].upper()
                    match =  foreign_key[7].upper()
                    if idx != 0 and foreign_key[0] != before_id:
                        str_1 = "\nFOREIGN KEY ("
                        str_2 = f") REFERENCES {before_reled_table_name}("
                        for ix, r1 in enumerate(rel_keys):
                            if ix == 0:
                                str_1 += r1
                            else:
                                str_1 += f",{r1}"
                        for ix, r2 in enumerate(reled_keys):
                            if ix == 0:
                                str_2 += r2
                            else:
                                str_2 += f",{r2}"
                        str_foreign_key += str_1+str_2+")"
                        if before_restrict_update and (before_restrict_update != "RESTRICT" and before_restrict_update != "NO ACTION"):
                            str_foreign_key += f" ON UPDATE {before_restrict_update}"
                        if before_restrict_delete and (before_restrict_delete != "RESTRICT" and before_restrict_delete != "NO ACTION"):
                            str_foreign_key += f" ON DELETE {before_restrict_delete}"
                        if before_match and before_match != "NONE":
                            str_foreign_key += f" MATCH {before_match}"
                        str_foreign_key += ","
                        rel_keys.clear()
                        reled_keys.clear()
                    rel_keys.append(foreign_key[3])
                    if foreign_key[4]:
                        reled_keys.append(foreign_key[4])
                    else:
                        reled_keys.append(foreign_key[3])
                    before_id = foreign_key[0]
                    before_reled_table_name = reled_table_name
                    before_restrict_update = restrict_update
                    before_restrict_delete = restrict_delete
                    before_match = match
                if before_reled_table_name and reled_keys and rel_keys:
                    str_1 = "\nFOREIGN KEY ("
                    str_2 = f") REFERENCES {before_reled_table_name}("
                    for idx, r1 in enumerate(rel_keys):
                        if idx == 0:
                            str_1 += r1
                        else:
                            str_1 += f",{r1}"
                    for idx, r2 in enumerate(reled_keys):
                        if idx == 0:
                            str_2 += r2
                        else:
                            str_2 += f",{r2}"
                    del rel_keys, reled_keys
                    str_foreign_key += str_1+str_2+")"
                    if before_restrict_update and (before_restrict_update != "RESTRICT" and before_restrict_update != "NO ACTION"):
                            str_foreign_key += f" ON UPDATE {before_restrict_update}"
                    if before_restrict_delete and (before_restrict_delete != "RESTRICT" and before_restrict_delete != "NO ACTION"):
                        str_foreign_key += f" ON DELETE {before_restrict_delete}"
                    if before_match and before_match != "NONE":
                        str_foreign_key += f" MATCH {before_match}"
                str_table_schema += str_foreign_key
            str_table_schema += "\n);"
            # pprint(str_table_schema)
            map_table_schema[table_name_src] = str_table_schema
            count_tables +=1
        map_db[db_name] = map_table_schema
        cursor.close()
        conn.close()
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(map_db, f, indent=2, ensure_ascii=False)
    print("db的数量:", len(map_db.keys()))
    print("表的数量:", count_tables)
    return map_db

def get_related_table_name(table_name:str, map_table_schema:Dict[str,str])->Optional[List[str]]:
    set_table_name = set()
    db_tables = list(map_table_schema.keys())
    for iter in db_tables:
        real_name = re.search(fr"\b{table_name}\b", iter, re.IGNORECASE)
        if real_name:
            table_name = real_name.group()
    set_table_name.add(table_name)
    table_schema = map_table_schema.get(table_name, None)
    # print(table_schema)
    if not table_schema:
        # print(table_name)
        return None
    else:
        table_schema += " "
        line_foreign_key= re.findall("(?<=FOREIGN KEY|foreign key)[ |\n]+?\(?(.*?)\)?[ |\n]+?(REFERENCES|references)[ |\n]+?([A-Za-z_'`\"]+)\((.+?)\)", table_schema)
        related_table_name = [re.search("([A-Za-z_]+)", t_n[2]).group(1) for t_n in line_foreign_key]
        set_table_name.update(related_table_name)
    return list(set_table_name)

def get_tables_in_sql(origanal_sql: str):
    origanal_sql += " "
    map_alias = {name[2]: name[0] for name in re.findall("([a-zA-Z_]+?)[ |\n]+?(as|AS)[ |\n]+?([a-zA-Z_0-9]+?)[ |\n]+?", origanal_sql)}
    set_table_name = set()
    t_1 = [re.search("[A-Za-z_0-9]+", i).group() for i in re.findall("(?<=FROM|from)[ |\n]+?\(?[a-zA-Z_0-9'`\"]+\)?[ |\n]+?",origanal_sql) if not re.search("select|SELECT", i)]
    t_2 = [re.search("[A-Za-z_0-9]+", j).group() for j in re.findall("(?<=JOIN|join)[ |\n]+?\(?[a-zA-Z_0-9'`\"]+\)?[ |\n]+?",origanal_sql) if not re.search("select|SELECT", j)]
    li_table_name = t_1+t_2
    for self_table in li_table_name:
        if self_table not in map_alias:
            set_table_name.add(self_table)
    return set_table_name
            
def get_related_table_schema(origanal_nl2sql: Dict[str,Any])->Dict[str,str]:
    origanal_sql = origanal_nl2sql["query"]
    map_table_schema = Map_db_table_schema.get(origanal_nl2sql["db_id"])
    set_table_name = get_tables_in_sql(origanal_sql)
    # print(set_table_name)
    li_table_name = list(set_table_name)
    for self_table in li_table_name:
        extent_table = get_related_table_name(self_table, map_table_schema)
        # print(extent_table)
        if not extent_table:
            continue
        set_table_name.update(extent_table)
    all_table_name = list(set_table_name)
    related_table_schema = {}
    for k in all_table_name:
        if not map_table_schema.get(k):
            continue
        related_table_schema[k] = map_table_schema[k]
    return related_table_schema
    
def build_nl2sql_prompt_template(origanal_nl2sql: Dict[str, str]):
    out_parser = PydanticOutputParser(pydantic_object=NlAndSQL)
    origanal_query = origanal_nl2sql["question"]
    origanal_sql = origanal_nl2sql["query"]
    # print(origanal_sql)
    table_schema = list(get_related_table_schema(origanal_nl2sql).values())
    # pprint(table_schema)
    """
    
    """
    prompt = f"""
### 相关联的表结构 ###
{table_schema}

### 原来的自然语言和SQL语句 ###
原来的自然语言：{origanal_query}
原来的SQL语句：{origanal_sql}

### SQL条件 ###
- * 连接表 *
- * 嵌套表 *
- 分组
- 排序（最大[最多]或最小[最少]）
- 存在性检查或空值判断
- 字符串类型的字段进行模糊匹配
- 数值类型的字段进行比较、运算或限定范围

### 任务 ###
在原来的自然语言基础上增加2-3个条件获得一个新自然语言，然后生成新自然语言对应的SQL语句。

### 实现步骤 ###
Step 1: 在原来的自然语言基础上随机增加2-3个### SQL条件 ###（不少于2个，其中尽量包含带星号*的条件），从而生成新自然语言。（要求语言准确、流畅、优雅，用中文表达）
Step 2: 使用### 相关联的表结构 ###中合适的表和字段，将新自然语言转换成对应的SQL语句（要求遵循MySQL语法规则，表名和字段名必须来自### 相关联的表结构 ###）

### 输出格式 ###
{out_parser.get_format_instructions()}
注意：必须按照### 输出格式 ###输出标准的JSON（包含输出格式中的所有的字段），不要输出任何多余的内容。
"""
    return prompt, out_parser

def comflex_sql_fliter(data_list: List[Dict[str, Any]]):
    filted_data_list = []
    for data in data_list:
        flex = 0
        sql = data.get("query")
        if not sql:
            raise Exception("SQL is NULL!")
        if "join" in sql or "JOIN" in sql:
            filted_data_list.append(data)
            continue
        elif len(re.findall("select|SELECT", sql)) > 2:
            filted_data_list.append(data)
            continue
        for k_w in ["GROUP BY", "HAVING", "ORDER BY", "LIMIT"]:
            if k_w in sql or k_w.lower() in sql:
                flex+=1
        if flex >= 2:
            filted_data_list.append(data)
    return filted_data_list

def extent_one_sample(data: Dict[str,Any], db_dir: str = "./data_example/train_databases"):
    sample = {
                "pair_id": data["pair_id"],
                "db_id": data["db_id"],
                "original_question": data["question"],
                "original_query": data["query"],
            }
    prompt, parser = build_nl2sql_prompt_template(data)
    # print(prompt)
    try:
        res = open_chat(prompt)
        response = parser.parse(repair_json(res)).model_dump()
        sample["question"] = response["query"]
        sample["query"] = response["sql"]
        query_sql = fix_table_name_in_sql(sample, db_dir)
        # query_sql = repair_sql(sample)
        return query_sql
    except:
        return None

async def a_extent_one_sample(data: Dict[str,Any], db_dir: str = "./data_example/train_databases"):
    sample = {
                "pair_id": data["pair_id"],
                "db_id": data["db_id"],
                "original_question": data["question"],
                "original_query": data["query"],
            }
    prompt, parser = build_nl2sql_prompt_template(data)
    # print(prompt)
    try:
        res = await a_open_chat(prompt)
        # print(res)
        response = parser.parse(repair_json(res)).model_dump()
        sample["question"] = response["query"]
        sample["query"] = response["sql"]
        query_sql = fix_table_name_in_sql(sample, db_dir)
        # query_sql = repair_sql(sample)
        return query_sql
    except:
        return None

def extent_datasets(configs):
    json_file_path = configs.src_nl2sql
    out_path = configs.dst_nl2sql
    db_dir = configs.databases_dir
    with open(json_file_path, "r", encoding="utf-8") as file:
        data_list = json.load(file)
    for index, sample in enumerate(data_list):
        sample["pair_id"] = index

    try:
        with open(out_path, "r", encoding="utf-8") as ff:
            extent_data = json.load(ff)
            exist_pair_ids = {i["pair_id"]: i for i in extent_data}
    except:
        extent_data = []
        exist_pair_ids = {}

    comflex_data_list = comflex_sql_fliter(data_list)
    if len(extent_data) == len(comflex_data_list):
        return extent_data
    print("复杂的查询-SQL对的数量:", len(comflex_data_list))

    # print([data for data in comflex_data_list[-10:] if data["pair_id"] not in exist_pair_ids])

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(extent_one_sample, *[data, db_dir]) for data in comflex_data_list[:40] if data["pair_id"] not in exist_pair_ids]
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Extenting Data"):
            one_sample = future.result()
            # print(one_sample)
            if one_sample and is_right_sql(one_sample, db_dir) and len(one_sample["query"]) > len(one_sample["original_query"]):
                extent_data.append(one_sample)
    extent_data = add_tabel_schema(extent_data)
    with open(out_path,"w",encoding="utf-8") as ff:
        json.dump(extent_data,ff,ensure_ascii=False,indent=2)
    # pprint(extent_data)
    print("扩充的查询-SQL对的数量:", len(extent_data))
    return extent_data

async def a_extent_datasets(configs):
    json_file_path = configs.src_nl2sql
    out_path = configs.dst_nl2sql
    db_dir = configs.databases_dir
    with open(json_file_path, "r", encoding="utf-8") as file:
        data_list = json.load(file)
    for index, sample in enumerate(data_list):
        sample["pair_id"] = index

    try:
        with open(out_path, "r", encoding="utf-8") as ff:
            extent_data = json.load(ff)
            exist_pair_ids = {i["pair_id"]: i for i in extent_data}
    except:
        extent_data = []
        exist_pair_ids = {}

    comflex_data_list = comflex_sql_fliter(data_list)
    if len(extent_data) == len(comflex_data_list):
        return extent_data
    print("复杂的查询-SQL对的数量:", len(comflex_data_list))
    
    count_n = len([1 for c_n in comflex_data_list[:5] if c_n["pair_id"] not in exist_pair_ids])
    p_bar = tqdm(desc="Extenting Data", total=count_n)
    flag_update = 0
    for idx, data in enumerate(comflex_data_list[:5]):
        if data["pair_id"] not in exist_pair_ids:
            one_sample = await a_extent_one_sample(*[data, db_dir])
            if one_sample and is_right_sql(one_sample, db_dir) and len(one_sample["query"]) > len(one_sample["original_query"]):
                extent_data.append(one_sample)
                flag_update += 1
                if flag_update+1 % 5 == 0:
                    with open(out_path,"w",encoding="utf-8") as ff:
                        json.dump(extent_data,ff,ensure_ascii=False,indent=2)
                p_bar.update(1)
            else:
                p_bar.total -= 1
                p_bar.refresh()
    extent_data = add_tabel_schema(extent_data)
    with open(out_path,"w",encoding="utf-8") as ff:
        json.dump(extent_data,ff,ensure_ascii=False,indent=2)
    # pprint(extent_data)
    print("扩充的查询-SQL对的数量:", len(extent_data))
    return extent_data

def add_tabel_schema(data: List[Dict[str,Any]]):
    for sample in data:
        table_schema = get_related_table_schema(sample)
        sample["all_tables"] = Map_db_table_schema[sample["db_id"]]
        sample["related_tables"] = table_schema
    return data


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NL2SQL_Extension")
    parser.add_argument("--databases_dir", type=str, default="./data_example/train_databases", help="数据库目录，包含所有的数据库文件。")
    parser.add_argument("--src_nl2sql", type=str, default="./data_example/train_v2.json", help="原NL2SQL数据集的json文件")
    parser.add_argument("--dst_nl2sql", type=str, default="./data_example/train_v2_extended.json", help="扩充的的json文件")
    args = parser.parse_args()

    Map_db_table_schema = get_map_all_table_schema_by_sqlite(args.databases_dir)
    if args.src_nl2sql != "./data_example/train_v2.json":
        full_name = os.path.basename(args.src_nl2sql)
        file_name = os.path.splitext(full_name)[0]
        new_name = file_name+'_extended.json'
        args.dst_nl2sql = os.path.join(os.path.dirname(args.src_nl2sql), new_name)
    pprint(args)

    extent_datasets(args)
    # asyncio.run(a_extent_datasets(args))




