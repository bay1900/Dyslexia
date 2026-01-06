from datetime import datetime
from requests.exceptions import RequestException, JSONDecodeError
from utils.file import read_csv, read_yaml
import requests
import time 
import json


# LOGGER
from utils.logger import get_logger
logger = get_logger(__name__)

# CONFIGURATION
config         = read_yaml("./config.yaml")
http_config    = config[1]["http"]
reddit_config  = config[1]["reddit"]

            
def extract_comment_data(data, post_id):
    """Helper: Extracts specific fields from a raw Reddit comment dictionary."""
    return {
        'kind_desc': 'comment',
        'reddit_id': data.get('id'),
        'post_id': post_id,
        'parent_id': data.get('parent_id'),
        'author': data.get('author', '[deleted]'),
        'timestamp_utc': data.get('created_utc'),
        'datetime': datetime.fromtimestamp(data['created_utc']).strftime('%Y-%m-%d %H:%M:%S') if data.get('created_utc') else 'N/A',
        'text': data.get('body', ''),
        'score': data.get('score', 0),
        'depth': data.get('depth', 0) 
    }

def get_comment(comments_data, post_id, depth=0):
    nest_cmm = []
        
    # SAFETY CHECK ( ENSURE COMMENTS DATA IS A LIST )
    if not isinstance(comments_data, list):
        return nest_cmm
    
    # LOGGER BASE DEPTH
    # Level 0 = "", Level 1 = "  ", Level 2 = "    "
    indent = "    " * depth
    
    for item in comments_data:
        # CHECK IF ITEM IS VALID
        if isinstance(item, dict) and 'kind' in item:
            kind = item['kind']
            data = item['data']

            # t1 IN REDDIT MEANS COMMENT
            if kind == 't1':
                
                # EXTRACT COMMENT DATA
                extract_cmm_data = extract_comment_data(data, post_id)
                nest_cmm.append(extract_cmm_data)
                
                # LOG THE COMMENT INFO
                logger.info(f"{indent}[L{depth}] -- {extract_cmm_data['reddit_id']} -- {extract_cmm_data['text'][:30]}...")
                
                # INITIALIZE LIST FOR NESTED CHILDREN
                nested_children = []
                
                # CHECK FOR NESTED REPLIES
                replies_raw = data.get('replies', '')
                if isinstance(replies_raw, dict) and 'data' in replies_raw:
                    
                    # GET NESTED CHILDREN COMMENTS
                    nested_children = replies_raw['data'].get('children', [])

                    # RECURSIVELY PROCESS NESTED CHILD COMMENTS
                    child_comments = get_comment(nested_children, post_id, depth + 1)
                    
                    # ADD CHILD COMMENTS TO THE MAIN LIST
                    nest_cmm.extend(child_comments)
                    
            # 'more' KIND HANDLES ADDITIONAL COMMENTS NOT LOADED INITIALLY ( THE LAZY LOADING )
            elif kind == 'more':
                pass

    return nest_cmm

def fetct_comment(CSV_ID, SUBREDDIT, posts):
    
    """
    ITERATE THROUGH EACH POST ID AND FETCH COMMENTS
    """

    # GATHER COMMENTS AND NESTED COMMENTS
    comments = []
    for i, post in enumerate(posts):

        # CURRENT POST ID
        current_post_id = post["reddit_id"]
        post_title = post.get("title", "Unknown Title")
        
        logger.info(f"------------------------------------------")
        logger.info(f"PROCESSING POST {i+1}/{len(posts)}: ID {current_post_id}")
        logger.info(f"Title: {post_title[:60]}...")
        logger.info(f"------------------------------------------")
        
        # MULTATE REDDIT COMMENT URL WITH CURRENT POST ID
        URL = f"{reddit_config["comment_base_url"].format(POST_ID = current_post_id)}"
        try:
            headers = {'User-Agent': http_config['User-Agent']}
            resp = requests.get(
                                    URL, 
                                    headers = headers, 
                                    timeout = http_config["timeout"] )
            
            # THE COMMENTS JSON RETURNS A LIST OF TWO ELEMENTS:
            # [0] IS THE POST DATA, [1] IS THE COMMENTS LISTING
            comments_data = resp.json()
            

            if (isinstance(comments_data, list) and len(comments_data) > 1 
                                                and 'data' in comments_data[1] 
                                                and 'children' in comments_data[1]['data']):
                
                # ROOT LEVEL CHILDREN COMMENTS
                root_children = comments_data[1]['data']['children']
                
                # RECURSIVELY PROCESS COMMENTS AND NESTED REPLIES
                nested_comment = get_comment(root_children, post["reddit_id"])
                
                # ADD TO THE MAIN COMMENTS LIST
                comments.extend(nested_comment)

        except requests.exceptions.RequestException as e:
            logger.info(f" - Error Fetch cmm  {post['reddit_id']}: {e}")
        except json.JSONDecodeError:
            logger.info(f" - Error decoding JSON for post {post['reddit_id']}.")
        except Exception as e:
            logger.info(f" - General error: {e}")

        # DELAY BETWEEN REQUESTS
        time.sleep(reddit_config["delay_between_requests"]) 

    return comments

def get_post(CSV_ID, SUBREDDIT, resp): 
    
    # CHECK RESPONSE CONTENT TYPE
    resp_content_type = resp.headers.get('Content-Type', '').lower()
    if 'application/json' in resp_content_type:
        
        # JSON RESPONSE
        posts_data = resp.json()
        
        # CHECK IF SUBREDDIT POSTS EXIST
        posts = []
        if 'data' in posts_data and 'children' in posts_data['data']:
            for post in posts_data['data']['children']:
                    
                post_data = post['data']
                kind      = post['kind']
                
                # SKIP STICKIED POSTS
                if post_data.get('stickied'):
                    continue
                
                # COLLECT POST DATA
                posts.append({
                    'csv_id' : CSV_ID,
                    'kind'   : kind,
                    'kind_desc' : 'post',
                    'reddit_id' : post_data['id'],
                    'title'  : post_data['title'],
                    'author' : post_data.get('author', '[deleted]'),
                    'timestamp_utc': post_data['created_utc'],
                    'datetime': datetime.fromtimestamp(post_data['created_utc']).strftime('%Y-%m-%d %H:%M:%S'),
                    'text'   : post_data.get('selftext', ''),
                    'score'  : post_data.get('score', 0),
                    'num_comments': post_data.get('num_comments', 0),
                    # 'url'    : post_data.get('url', '')
                })
        else:
            logger.warning(f" CSV ID {CSV_ID} - SUBREDDIT {SUBREDDIT} - No posts found in the subreddit response.")
    else:
        logger.warning(" CSV ID {CSV_ID} - SUBREDDIT {SUBREDDIT} - unexpected JSON structure.")     
    
    return posts
            
def fetch_post( CSV_ID, SUBREDDIT ):
    
    # REDDIT POST URL 
    URL = f"{reddit_config["post_base_url"].format(SUBREDDIT = SUBREDDIT)}"
    
    all_posts = []
    try:
        # HTTP REQUEST
        headers = {'User-Agent': http_config['User-Agent']}
        resp = requests.get(    URL,
                                headers = headers, 
                                timeout = http_config["timeout"] )
        
        # HTTP RESPONSE CODE
        # 200 = SUCCESS
        # 429 = TOO MANY REQUESTS
        resp_code = resp.status_code
        if resp_code == 200 or resp_code == 429:
            posts  =  get_post(CSV_ID, SUBREDDIT, resp)
            all_posts.extend(posts)
            logger.info(f"  ~~~~~ CSV ID {CSV_ID} - CODE {resp_code} - SUBREDDIT {SUBREDDIT} - Found {len(posts)} posts non-stickied ~~~~~")
        else:
            logger.error(f"  ~~~~~ CSV ID {CSV_ID} - CODE {resp_code} - SUBREDDIT {SUBREDDIT} - Found 0 posts non-stickied ~~~~~")
         
    except RequestException as e:
        logger.error(f" \n ~~~~~ CSV ID {CSV_ID} - Fail Request - SUBREDDIT {SUBREDDIT} - {e}")
    except JSONDecodeError:
        logger.error(f" \n ~~~~~ CSV ID {CSV_ID} - Failed To Decode JSON - SUBREDDIT {SUBREDDIT}")
    except Exception as e:
        logger.error(f" \n ~~~~~ CSV ID {CSV_ID} - Unexpected Error - SUBREDDIT {SUBREDDIT} - {e}")
    
    return  all_posts