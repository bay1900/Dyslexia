import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
import json
import os 
from utils.file import read_csv, read_yaml, write_json
from gensim.parsing.preprocessing import STOPWORDS # STOP WORD LIST

# STOP WORDS
stopwords = set(STOPWORDS)

# LOGGER
from utils.logger import get_logger
logger = get_logger(__name__)

# CONFIGURATION
config         = read_yaml("./config.yaml")
http_config    = config[1]["http"]
cleaning_config= config[1]["cleaning"]

def text_process(text): 
    text = str(text).lower()            # LOWERCASE TEXT
    text = re.sub(r'https?://\S+|www\.\S+', '', text) # REMOVE LINKS
    text = re.sub(r'[^\w\s]', '', text) # REMOVE PUNCTUATION
    
    # REMOVE STOPWORDS
    words = text.split()
    text = [w for w in words if w not in stopwords] # REMOVE STOPWORDS
    return " ".join( text )


# GET ALL CSV FILES IN OUTPUTFOLDER
folder_path = './data/out/'
file_names = [] 
logger.info(f"-- scanning folder for csv files : {folder_path} --")
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        file_names.append(file_path)
logger.info(f"-- finished scanning folder, found {len(file_names)} csv files --")

# INITIALIZE REPORT DICTIONARY
report = { "info": None,
           "sumary": None,
           "data": [],
           "hist": [] }

df_concatenated = []
for i, file in enumerate(file_names):
    # 1. INITIALIZE LOG
    processing_log = []
    
    # ************** STEP 1 **************

    # LOAD DATASET
    df = pd.read_csv( file )
    posts_total = len(df)
    # LOG STEP 1: INITIAL LOAD
    processing_log.append({
        "step": "1_load_data",
        "description": "Raw data loaded from CSV",
        "rows_remaining": len(df),
        "rows_removed": 0
    })
    
    data_info   = {}
    config_info = {}
    
    if i == 0:
        logger.info("-- gathering dataset overview information --")
        # DATASET OVERVIEW
        attributes     = df.columns.tolist()
        num_attributes = len(attributes)
        num_posts      = len(df)
        
        config_info                    = cleaning_config
        config_info["attribute_count"] = num_attributes
        config_info["attributes"]      = attributes      # NOT INCLUDE TEMPERARY COLUMN "word_count"
        
        report["info"] = config_info
        logger.info(f"-- dataset overview information gathered --")
    
    # ************** STEP 2 **************
    
    # parent_id COLUMN IN CSV INDICATE STATUS OF EACH DATA ROW
    # parent_id = NULL     = POST
    # parent_id = NOT NULL = REPLY
    replies   = int(df['parent_id'].notna().sum())     # REPLIES
    posts     = int(posts_total - replies) # POSTS TITLE
    reply_pct = round( float((replies / posts_total) * 100 ), 2 )  # REPLIES IN %
    posts_pct = round(float(100 - reply_pct), 2)                 # POST IN %

    # ADD word_count COLUMN
    # REMOVE POST LENGTH LESS THAN 3 WORDS 
    logger.info("**")
    logger.info(f"cleaning dataset : removing short posts and specific authors")
    
    df['word_count'] = df['text'].fillna('').apply(lambda x: len(str(x).split()))
    short_post = df['word_count'] < cleaning_config["word_length"]
    short_post_count = int(short_post.sum())
    short_post_pct   = round( float((short_post_count / posts_total) * 100 ), 2 )
    # ERASE SHORT POSTS
    df = df[~short_post]
    
    processing_log.append({
        "step": "2_rmd_short_posts",
        "description": f"Removed posts with fewer than {cleaning_config["word_length"]} words",
        "rows_remaining": len(df),
        "rows_removed": int(short_post_count)
    })
    
    # ************** STEP 3 **************
    
    # ERASE POSTS FROM SPECIFIC AUTHORS
    # [deleted] = DATA DELETED BY USER
    # [removed] = DATA REMOVED BY MODERATOR
    post_rm_author     = int(0)
    post_rm_author_pct = round( float(0), 2 )
    if cleaning_config["author_filter_trigger"]:
        post_rm_author     = int(df['author'].isin(cleaning_config["author_filter"]).sum())
        post_rm_author_pct = round( float((post_rm_author / num_posts) * 100 ), 2 )
        df = df[~df['author'].isin(cleaning_config["author_filter"])] 
    
    logger.info(f"removed {short_post_count} short posts with less than {cleaning_config['word_length']} words")
    
    processing_log.append({
        "step": "3_rmd_author",
        "description": f"Removed posts from specific authors ( reddit specific )",
        "rows_remaining": len(df),
        "rows_removed": int(post_rm_author)
    })
    
    
    # ************** STEP 4 **************
    
    # LOWER TEXT, REMOVE PUNCTUATION, REMOVE STOP WORDS
    textprocess_count_before = int(df['text'].str.split().str.len().sum())
    df["text"]        = df["text"].apply( text_process )
    textprocess_count_after  = int(df['text'].str.split().str.len().sum())
    
    print ( textprocess_count_before, " - ", textprocess_count_after )
    
    processing_log.append({
        "step": "4_text_process",
        "description": f"Lower case text, Removed links, punctuation, stopwords",
        "rows_remaining": len(df),
        "rows_removed": 0,
        "text_process": { 
            "count_before" : textprocess_count_before, 
            "count_after"  : textprocess_count_after
        }
    })

    df_concatenated.append(df)

    # REPORT DATA
    data_info["csv_id"]      = "out_DyslexicParents"
    data_info["subreddit"]   = "out_DyslexicParents"
    data_info["post_total"]  = posts_total
    data_info["posts"]       = posts
    data_info["posts_pct"]   = posts_pct
    data_info["replies"]     = replies
    data_info["replies_pct"]   = reply_pct
    data_info["post_rmd_short"]  = short_post_count
    data_info["post_rmd_short_pct"]  = short_post_pct
    data_info["post_rmd_author"] = post_rm_author
    data_info["post_rmd_author_pct"] = post_rm_author_pct
    
    report["data"].append(data_info)
    
    # --- SAVE THE LOG ---
    report["hist"].append ({ 
            "file": file,
            "history": processing_log
                           })
    
    logger.info(f"finished cleaning dataset : {file}")
    
logger.info("**")


# REPORT SUMMARY
logger.info("generating cleaning report summary")
post_total = [] 
posts = []
replies = []
post_rmd_short = []
post_rmd_author= []

for i, report_data in enumerate(report["data"]):
    
   post_total.append(report_data["post_total"])
   posts.append(report_data["posts"])
   replies.append(report_data["replies"])
   post_rmd_short.append(report_data["post_rmd_short"])
   post_rmd_author.append(report_data["post_rmd_author"])

report["sumary"] = {
                    "post_total": sum(post_total),
                    "posts": sum(posts),
                    "posts_pct": round( (sum(posts) / sum(post_total)) * 100 , 2 ),
                    "replies": sum(replies),
                    "replies_pct": round( (sum(replies) / sum(post_total)) * 100 , 2 ),
                    "post_rmd_short": sum(post_rmd_short),
                    "post_rmd_short_pct": round( (sum(post_rmd_short) / sum(post_total)) * 100 , 2 ),   
                    "post_rmd_author": sum(post_rmd_author),
                    "post_rmd_author_pct": round( (sum(post_rmd_author) / sum(post_total)) * 100 , 2 )
                   }   
logger.info("finished generating cleaning report summary")


# print ( "report : " , json.dumps(report, indent=4) )
write_json ( cleaning_config["output_report"], report)

# SAVE TO CSV
logger.info("exporting cleaned and concatenated dataframe to csv")
xdf = pd.concat(df_concatenated, ignore_index=True) 
output_csv = f'{cleaning_config["output_path"]}'
xdf.to_csv(output_csv, index=False) # EXPORT DF AFTER CLEANING
logger.info(f"exported cleaned and concatenated dataframe to csv : {output_csv}")
