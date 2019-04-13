import datetime
import io
import json
import logging
import os
import string
from datetime import datetime, timedelta, timezone

import gspread
import ipdb
import numpy as np
import pandas as pd
import requests
from dateutil import relativedelta
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize

from .search_ads_two import Client