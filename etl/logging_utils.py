import logging, os
 
def get_logger(name="ecom_etl", log_file="logs/etl.log"): 
    os.makedirs(os.path.dirname(log_file), exist_ok=True) 
    logger = logging.getLogger(name) 
    logger.setLevel(logging.INFO) 
    if not logger.handlers: 
        fh = logging.FileHandler(log_file) 
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s") 
        fh.setFormatter(fmt) 
        logger.addHandler(fh) 
    return logger 