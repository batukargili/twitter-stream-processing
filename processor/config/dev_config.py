import sys
import json

try:
    config_json_path = sys.argv[1]
    with open(config_json_path, 'r') as f:
        json_config = json.load(f)
    print(config_json_path)

except Exception as ex:
    config_json_path = "processor/config/processor_config.json"
    with open(config_json_path, 'r') as f:
        json_config = json.load(f)
    print('WARNING')
    print("Default conf file processor_config.json in use because you did not pass json file as argument")

spark_loglevel = json_config['spark_loglevel']
tcp_host = json_config['tcp_host']
tcp_port = json_config['tcp_port']
mongo_host = json_config['mongo_host']
mongo_port = json_config['mongo_port']
mongo_database = json_config['mongo_database']
mongo_collection = json_config['mongo_collection']
covid_source_url = json_config['covid_source_url']

