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

def instance_to_array(instance):
    return [value for key, value in instance.__dict__.items()]

mc_factor = pd.read_csv("bookable_search.csv")
def get_mc_factor(calendar_date: str):
            
            
    date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%a")
    month = date_obj.strftime("%B")
    q = f'Month == "{month}" & Day == "{day_of_week}"'
    factor = mc_factor.query(q)
            
    return factor.Bookable_Search.iloc[0]

def calculate_price(dat, choice = 1):
            
    m = dat.copy()
    m['price'] = m.price.astype(str)
    m['price'] = m['price'].str.replace('$', '')
    m['price'] = m['price'].str.replace(',', '')
    mat =  m.iloc[:,:10].values.astype(float)
    dynasaur = PriceModel(market_matrix = mat, coeff = [-0.0062, 0.0003, 0.0879, 0.1106, 0.3239, 0.015, 0.0002, 0.011, 0.42, 0.141], mc = choice)
    res = dynasaur.optimize()
    m["Optimized_Price"] = 0
    i = 0  
    j = "Optimized_Price"
    m.at[i, j] = res[1]
    
    return m

def calculate_share(dat, choice = 1):
            
    m = dat.copy()
    m['price'] = m.price.astype(str)
    m['price'] = m['price'].str.replace('$', '')
    m['price'] = m['price'].str.replace(',', '')
    mat =  m.iloc[:,:10].values.astype(float)
    dynasaur = PriceModel(market_matrix = mat, coeff = [-0.0062, 0.0003, 0.0879, 0.1106, 0.3239, 0.015, 0.0002, 0.011, 0.42, 0.141], mc = choice)
    res = dynasaur.compute_share()
    m["Market_Share"] = res
    
    return m

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

    def optimize_price(self,listing_id: str, calendar_date: str):
        
        optimized_price = None
        
        market_data = self.build_matrix(listing_id,calendar_date)
        market_data = market_data.drop_duplicates(subset = ['id'])
        market_data["ToOptimize"] = market_data['id'].apply(lambda x: 1 if str(x) == str(listing_id) else 0)
        market_data = market_data.query('available == True or ToOptimize == 1')
        market_data = market_data.sort_values(by = 'ToOptimize', ascending = False)
        to_optimize = (market_data['ToOptimize'] == 1).any()
        num_comp = market_data.shape[0]
        if to_optimize and num_comp > 1:
            i = float(market_data.iloc[0]["mc"])
            optimized_price = calculate_price(market_data,i)
        
        return optimized_price
            

    def compute_share(self,property_info: PropertyAttribute, calendar_date: str):

    
        market_share = 0
        listing_id = property_info._id

        market_data = self.build_matrix(listing_id,calendar_date)
        market_data = market_data.drop_duplicates(subset = ['id'])
        market_data["ToOptimize"] = market_data['id'].apply(lambda x: 1 if str(x) == str(listing_id) else 0)
        market_data = market_data.query('available == True or ToOptimize == 1')
        market_data = market_data.sort_values(by = 'ToOptimize', ascending = False)
        property_attr = instance_to_array(property_info)
        market_data.iloc[0, 0:11] = property_attr
        to_optimize = (market_data['ToOptimize'] == 1).any()
        num_comp = market_data.shape[0]
        if to_optimize and num_comp > 1:
            i = float(market_data.iloc[0]["mc"])
            market_share = calculate_share(market_data,i)
        

        return market_share


    def build_matrix(self,listing_id: str, calendar_date: str):
        
        
        client_property_data = get_property_info([listing_id])[0]
        rental_market = ClientProperty(id = client_property_data["listing_id"],competitors = client_property_data["intelCompSet"])

        all_ids = []
        all_ids.append(client_property_data["listing_id"])
        all_ids.extend(client_property_data["intelCompSet"])
        all_ids = list(set(all_ids))


        image_set = get_image_scores(all_ids)
        image_set = pd.DataFrame(image_set)
        image_scores = image_set.groupby('Listings')['Score'].agg(list).reset_index()
        image_scores.columns = ["id","Scores"]
        image_scores["Values"] = image_scores.apply(odd_weighted_average, axis=1)
        image_scores[['Reference', 'Adjusted', 'Factor']] = image_scores['Values'].apply(lambda x: pd.Series(x))
        image_scores = image_scores[["id","Scores","Reference","Adjusted"]]


        comp_list = []
        [comp_list.append(x) for x in rental_market._competitors]
                

        client_listing = pd.DataFrame(get_listing_info([listing_id]))
        competitor_listing = pd.DataFrame(get_listing_info(comp_list))
        market_listing = pd.concat([client_listing,competitor_listing],axis = 0)
        market_listing["bedrooms"] = pd.to_numeric(market_listing["bedrooms"], errors="coerce").fillna(0).astype(int)
        market_listing.rename(columns={'_id': 'listing_hashId'}, inplace=True)
        market_listing = parse_scrap_info(market_listing)
        market_listing = pd.merge(market_listing,image_scores, on = "id", how = "outer")
        market_listing["Reference"].fillna(market_listing["Reference"].mean(),inplace = True)
        market_listing["Adjusted"].fillna(market_listing["Adjusted"].mean(),inplace = True)


        market_availabilities = pd.DataFrame(get_availability_info(all_ids,[calendar_date]))
        #market_listing= pd.merge(market_listing,market_availabilities,on="id", how = 'outer')
        market_listing= pd.merge(market_listing,market_availabilities,on="id", how = 'inner')
        market_listing['dist'] = 0


        market_data = market_listing[["price","review_count","Adjusted","bedrooms","rating_value","minNights","dist","pool","jacuzzi","landscape_views","id","available","calendarDate","listing_hashId"]]
        market_data["mc"] = market_data["calendarDate"].apply(get_mc_factor)
        
        return market_data




