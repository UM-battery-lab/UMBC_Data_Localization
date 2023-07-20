# TODO: Use Voltaiq API to fetch TestRecord data
import voltaiq_studio as vs
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
import datetime
import os.path
from voltaiq_studio import TraceFilterOperation
plt.rcParams['figure.max_open_warning']=False
CRED = "\033[41m"
CEND = "\033[0m"
# %matplotlib widget

#datadict={'Date': [], 'Cell Number': [], 'discharge Cap': []}
datadict={'Date': [], 'Cell Number': [], 'discharge Cap': [],'Drtfreq1':[],'DrtR1':[],'Drtfreq2':[],'DrtR2':[],'Drtfreq3':[],'DrtR3':[]}
# import UMBCLCycleMetrics

import scipy as sp
import scipy.integrate
import bisect

# Enviroment Variables:
from dotenv import load_dotenv
import os

load_dotenv('voltaiq_env.env')

# Load all test records
trs = vs.get_test_records()
