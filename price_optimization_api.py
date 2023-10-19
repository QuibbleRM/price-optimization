#!/usr/bin/env python
# coding: utf-8
from src.dynapee import PriceModel
from src.datastruct import *
from src.utils import *
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta
import abc


mc_factor = pd.read_csv("bookable_search.csv")
def get_mc_factor(calendar_date: str):
            
            
            date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
            day_of_week = date_obj.strftime("%a")
            month = date_obj.strftime("%B")
            q = f'Month == "{month}" & Day == "{day_of_week}"'
            factor = mc_factor.query(q)
            
            return factor.Bookable_Search.iloc[0]


class IPriceAPI(abc.ABC):
    
    @abc.abstractclassmethod
    def optimize_price():
        pass
    
    @abc.abstractclassmethod
    def compute_share():
        pass

    @abc.abstractclassmethod
    def build_matrix():
        pass

    


class PriceAPI(IPriceAPI):

    def optimize_price(listing_id: str, calendar_date: str):
        pass

    def compute_share(property_info: PropertyAttribute):
        pass

    def build_matrix(listing_id: str, calendar_date: str):
        pass
        
        