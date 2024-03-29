import logging

import pandas as pd


def initialize():
    global current_dir
    current_dir = '/'
    global history
    history = pd.DataFrame(columns=['success', 'failure'])
    logging.basicConfig(format='%(asctime)s -%(levelname)s - %(message)s ', datefmt='%d-%b-%y %H:%M:%S')
    logging.root.setLevel(logging.ERROR)
    logging.disable(logging.CRITICAL)
    global cursor
    global connection
    global port
    port = 4001
