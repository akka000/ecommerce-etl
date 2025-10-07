import os, json
 
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) 
CONF_PATH = os.path.join(BASE_DIR, "config", "config.json") 
def load_config(): 
    with open(CONF_PATH) as f: 
        return json.load(f) 