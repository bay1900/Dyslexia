import pandas as pd
import json
import time

from utils.file import read_csv, read_yaml, write_json
from utils.extract import fetch_post, fetct_comment

# LOGGER
from utils.logger import get_logger
logger = get_logger(__name__)

# CONFIGURATION
config         = read_yaml("./config.yaml")
http_config    = config[1]["http"]
reddit_config  = config[1]["reddit"]
file_config    = config[1]["file_path"]
report_config  = config[1]["report"]

print ( file_config["input_csv"] )

# LOAD CSV INPUT
input_csv = read_csv(file_config["input_csv"])
# PARSE JSON STR TO A PYTHON LIST
data = json.loads(input_csv[1])

report = []

for csv_item in data:
    
    # START TIMER
    start_execute = time.time()
    
    SUBREDDIT = csv_item["SUBREDDIT"]
    CSV_ID    = csv_item["ID"]
    
    # FETCH POSTS AND COMMENTS
    posts    = fetch_post(CSV_ID, SUBREDDIT)
    comments = fetct_comment(CSV_ID, SUBREDDIT, posts)
    
    # MERGE POSTS AND COMMENTS
    merged_data = posts + comments
    
    # END TIMER
    # CALCULATE EXECUTION TIME
    end_execute = time.time() 
    execute_time = round(end_execute - start_execute, 2)
    
    # REPORT STATUS
    status = { 
              "subreddit" : SUBREDDIT, 
              "num_posts": len(posts), 
              "num_comments": len(comments),
              "total_records": len(merged_data),
              "csv_id": CSV_ID,
              "status": None,
              "status_desc": None,
              "execute_time": execute_time
            }
    
    # IN CASE OF PORVIDED SUBREDDIT HAS NO POSTS OR COMMENTS
    # AVOID CREATING EMPTY DATAFRAMES
    # SAVE REPORT
    # SKIP TO NEXT ITEM
    if len(merged_data) == 0:
        status["status"] = False
        status["status_desc"] = "No posts or comments found."
        report.append(status)
        continue
   
    # CREATE DATAFRAME
    df = pd.DataFrame( merged_data )

    # SAVE TO CSV
    output_csv = f'{file_config["output_csv"]}/out_{csv_item["SUBREDDIT"]}.csv'
    df.to_csv(output_csv, index=False)
    
    # UPDATE REPORT STATUS    
    status["status"] = True
    status["status_desc"] = "Data successfully extracted and saved."
    report.append(status)
    
# SAVE REPORT AS JSON
report_config_file = report_config["output_file"]
write_json(report_config_file, report)