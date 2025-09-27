import requests
import re
from pprint import pprint
from concurrent.futures import as_completed, ProcessPoolExecutor
from tqdm import tqdm
import asyncio
import json


# sql_input = {
#     "pair_id": 4354,
#     "db_id": "tracking_grants_for_research",
#     "original_question": "项目的第一批员工是什么时候开始工作的？",
#     "original_query": "SELECT date_from FROM Project_Staff ORDER BY date_from ASC LIMIT 1",
#     "question": "项目的第一批员工在哪个项目中开始工作，并且他们的开始日期不为空且早于2023年1月1日？",
#     "query": "SELECT ps.project_id, ps.date_from FROM Project_Staff ps WHERE ps.date_from IS NOT NULL AND ps.date_from < '2023-01-01' ORDER BY ps.date_from ASC LIMIT 1",
#     "all_tables": {
#       "Document_Types": "CREATE TABLE Document_Types (\ndocument_type_code VARCHAR(10),\ndocument_description VARCHAR(255) NOT NULL,\nPRIMARY KEY (document_type_code),\n);",
#       "Documents": "CREATE TABLE Documents (\ndocument_id INTEGER,\ndocument_type_code VARCHAR(10),\ngrant_id INTEGER NOT NULL,\nsent_date DATETIME NOT NULL,\nresponse_received_date DATETIME NOT NULL,\nother_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (document_id),\nFOREIGN KEY (grant_id) REFERENCES Grants(grant_id),\nFOREIGN KEY (document_type_code) REFERENCES Document_Types(document_type_code)\n);",
#       "Grants": "CREATE TABLE Grants (\ngrant_id INTEGER,\norganisation_id INTEGER NOT NULL,\ngrant_amount DECIMAL(19,4) NOT NULL DEFAULT 0,\ngrant_start_date DATETIME NOT NULL,\ngrant_end_date DATETIME NOT NULL,\nother_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (grant_id),\nFOREIGN KEY (organisation_id) REFERENCES Organisations(organisation_id)\n);",
#       "Organisation_Types": "CREATE TABLE Organisation_Types (\norganisation_type VARCHAR(10),\norganisation_type_description VARCHAR(255) NOT NULL,\nPRIMARY KEY (organisation_type),\n);",
#       "Organisations": "CREATE TABLE Organisations (\norganisation_id INTEGER,\norganisation_type VARCHAR(10) NOT NULL,\norganisation_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (organisation_id),\nFOREIGN KEY (organisation_type) REFERENCES Organisation_Types(organisation_type)\n);",
#       "Project_Outcomes": "CREATE TABLE Project_Outcomes (\nproject_id INTEGER NOT NULL,\noutcome_code VARCHAR(10) NOT NULL,\noutcome_details VARCHAR(255),\nFOREIGN KEY (outcome_code) REFERENCES Research_Outcomes(outcome_code),\nFOREIGN KEY (project_id) REFERENCES Projects(project_id)\n);",
#       "Project_Staff": "CREATE TABLE Project_Staff (\nstaff_id DOUBLE,\nproject_id INTEGER NOT NULL,\nrole_code VARCHAR(10) NOT NULL,\ndate_from DATETIME,\ndate_to DATETIME,\nother_details VARCHAR(255),\nPRIMARY KEY (staff_id),\nFOREIGN KEY (role_code) REFERENCES Staff_Roles(role_code),\nFOREIGN KEY (project_id) REFERENCES Projects(project_id)\n);",
#       "Projects": "CREATE TABLE Projects (\nproject_id INTEGER,\norganisation_id INTEGER NOT NULL,\nproject_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (project_id),\nFOREIGN KEY (organisation_id) REFERENCES Organisations(organisation_id)\n);",
#       "Research_Outcomes": "CREATE TABLE Research_Outcomes (\noutcome_code VARCHAR(10),\noutcome_description VARCHAR(255) NOT NULL,\nPRIMARY KEY (outcome_code),\n);",
#       "Research_Staff": "CREATE TABLE Research_Staff (\nstaff_id INTEGER,\nemployer_organisation_id INTEGER NOT NULL,\nstaff_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (staff_id),\nFOREIGN KEY (employer_organisation_id) REFERENCES Organisations(organisation_id)\n);",
#       "Staff_Roles": "CREATE TABLE Staff_Roles (\nrole_code VARCHAR(10),\nrole_description VARCHAR(255) NOT NULL,\nPRIMARY KEY (role_code),\n);",
#       "Tasks": "CREATE TABLE Tasks (\ntask_id INTEGER,\nproject_id INTEGER NOT NULL,\ntask_details VARCHAR(255) NOT NULL,\neg Agree Objectives VARCHAR(1),\nPRIMARY KEY (task_id),\nFOREIGN KEY (project_id) REFERENCES Projects(project_id)\n);"
#     },
#     "related_tables": {
#       "Project_Staff": "CREATE TABLE Project_Staff (\nstaff_id DOUBLE,\nproject_id INTEGER NOT NULL,\nrole_code VARCHAR(10) NOT NULL,\ndate_from DATETIME,\ndate_to DATETIME,\nother_details VARCHAR(255),\nPRIMARY KEY (staff_id),\nFOREIGN KEY (role_code) REFERENCES Staff_Roles(role_code),\nFOREIGN KEY (project_id) REFERENCES Projects(project_id)\n);",
#       "Projects": "CREATE TABLE Projects (\nproject_id INTEGER,\norganisation_id INTEGER NOT NULL,\nproject_details VARCHAR(255) NOT NULL,\nPRIMARY KEY (project_id),\nFOREIGN KEY (organisation_id) REFERENCES Organisations(organisation_id)\n);",
#       "Staff_Roles": "CREATE TABLE Staff_Roles (\nrole_code VARCHAR(10),\nrole_description VARCHAR(255) NOT NULL,\nPRIMARY KEY (role_code),\n);"
#     }
#   }

def convert_mysql_to_doris(data_sample, url="https://play.selectdb.com/sql-convertor-api/v1/convert"):
    convert_sample = {
        "pair_id": data_sample["pair_id"],
        "db_id": data_sample["db_id"],
        "original_question": data_sample["original_question"],
        "question": data_sample["question"],
        "all_tables_mysql": data_sample["all_tables"],
        "related_tables": list(data_sample["related_tables"].keys()),
        "original_query_mysql": data_sample["original_query"],
        "query_mysql": data_sample["query"]
    }
    header = {'Content-Type':'application/json'}
    data = {
        "sql_query": '',
        "from":"mysql",
        "to":"doris",
        "version":"1.0.1",
        "source":"text",
        "case_sensitive":"0"
    }
    sqls = ["original_query", "query"]
    tables = list(data_sample["all_tables"].keys())
    for sl in sqls:
        data["sql_query"] = re.sub("\n", "", data_sample[sl])
        res = requests.post(url, headers=header, json=data)
        response_sql = res.json()['data']['transformedSQL']
        convert_sample[f"{sl}_doris"] = response_sql
    all_tables_doris = {}
    for tl in tables:
        data["sql_query"] = re.sub("\n", "", data_sample["all_tables"][tl])
        res = requests.post(url, headers=header, json=data)
        response_sql = res.json()['data']['transformedSQL']
        all_tables_doris[tl] = response_sql
    convert_sample["all_tables_doris"] = all_tables_doris
    # pprint(convert_sample)
    return convert_sample

def process_one_sample(data_sample):
    try:
        res = convert_mysql_to_doris(data_sample)
        return res
    except:
        # print(data_sample)
        # print(res)
        # exit()
        return None

def convert_total_data(in_json_path: str, out_json_path: str):
    with open(in_json_path, 'r', encoding='utf-8') as f:
        total_data = json.load(f)
    print("查询-SQL对的总数量:", len(total_data))
    for index, samp in enumerate(total_data):
        samp["pair_id"] = index

    try:
        with open(out_json_path, "r", encoding="utf-8") as file:
            exist_data = json.load(file)
            exist_pair_ids = {i["pair_id"]: i for i in exist_data}
    except:
        exist_data = []
        exist_pair_ids = {}

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_one_sample, data) for data in total_data if data["pair_id"] not in exist_pair_ids]
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Translating Data"):
            one_sample = future.result()
            if one_sample:
                exist_data.append(one_sample)
            
    with open(out_json_path, 'w', encoding='utf-8') as ff:
        json.dump(exist_data,ff,ensure_ascii=False,indent=2)
    print("已转换的数量:", len(exist_data))
    return exist_data

if __name__ == "__main__":
    # convert_mysql_to_doris(sql_input)
    convert_total_data('./data_example/train_v2_extended.json', './data_example/train_v2_doris.json')