#!/usr/bin/env python
# coding: utf-8


from src.dynapee import PriceModel
from src.datastruct import ClientProperty
from src.utils import *
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta
from multiprocessing import Pool
import sys

arguments = sys.argv[1:] 

email_id = [arguments[0]]
offset = int(arguments[1])
workers = int(arguments[2])
mode = arguments[3]

if mode == None or mode == '':
    mode = 'prod'


user_ids = get_user_ids(email_id,mode)


calendar_dates = []
today = datetime.today() 
one_year_from_today = today + timedelta(days=offset)
date_strings = []
current_date = today
while current_date <= one_year_from_today:
    date_strings.append(current_date.strftime("%Y-%m-%d"))
    current_date += timedelta(days=1)

for date_str in date_strings:
    calendar_dates.append(date_str)


#client_property_data = get_property_info(client_property_ids)
client_property_data = get_property_info_by_user(user_ids,mode)


rental_market = []
comp_distr = []
for p_dat in client_property_data:
    comp_distr.append(len(p_dat["intelCompSet"]))
    rental_market.append(ClientProperty(id = p_dat["listing_id"],competitors = p_dat["intelCompSet"]))


for_image_scoring = []
for p_dat in client_property_data:
    for_image_scoring.extend([p_dat["listing_id"]])
    for_image_scoring.extend(p_dat["intelCompSet"])


image_set = get_image_scores(list(set(for_image_scoring)))


image_set = pd.DataFrame(image_set)


image_scores = image_set.groupby('Listings')['Score'].agg(list).reset_index()


image_scores.columns = ["id","Scores"]


image_scores["Values"] = image_scores.apply(odd_weighted_average, axis=1)



image_scores[['Reference', 'Adjusted', 'Factor']] = image_scores['Values'].apply(lambda x: pd.Series(x))
image_scores = image_scores[["id","Scores","Reference","Adjusted"]]



market_listing_collection = []

market_listing_daily = {}
_compList = []
_ids = [rental._id for rental in rental_market]
_comp = [rental._competitors for rental in rental_market]
[_compList.extend(x) for x in _comp]
        

client_listing = pd.DataFrame(get_listing_info(_ids))
competitor_listing = pd.DataFrame(get_listing_info(_compList))
market_listing = pd.concat([client_listing,competitor_listing],axis = 0)
market_listing["bedrooms"] = pd.to_numeric(market_listing["bedrooms"], errors="coerce").fillna(0).astype(int)
market_listing.rename(columns={'_id': 'listing_hashId'}, inplace=True)
market_listing = parse_scrap_info(market_listing)
market_listing = pd.merge(market_listing,image_scores, on = "id", how = "outer")
market_listing["Reference"].fillna(market_listing["Reference"].mean(),inplace = True)
market_listing["Adjusted"].fillna(market_listing["Adjusted"].mean(),inplace = True)


all_ids = [str(x) for x in list(market_listing.id)]
market_availabilities = pd.DataFrame(get_availability_info(all_ids,calendar_dates))
market_listing= pd.merge(market_listing,market_availabilities,on="id", how = 'outer')
market_listing['dist'] = 0




mc_factor = pd.DataFrame(get_user_mc_factor(email_id))

mc_factor['Bookable_Search'] = mc_factor.Bookable_Search.astype(float)

def get_mc_factor(calendar_date: str):
    
    mc = 1
    date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%a")
    ## changed to lower B for bookable_search of posto
    month = date_obj.strftime("%B")
    q = f'Month == "{month}" & Day == "{day_of_week}"'
    factor = mc_factor.query(q)
    if not factor.empty:
        factor = factor.to_dict(orient='records')
        factor = factor[0]['Bookable_Search']
        mc = factor
    return mc



market_data = []
for m in rental_market:
    tmp_ids = None
    tmp_ids = m._competitors.copy()
    tmp_ids.append(m._id)
    
    tmp = market_listing[market_listing.id.isin(tmp_ids)]
    for d in calendar_dates: 
        _tmp = tmp[tmp["calendarDate"] == d]
        _tmp = _tmp[["price","review_count","Adjusted","bedrooms","rating_value","minNights","dist","pool","jacuzzi","landscape_views","available","id","calendarDate","listing_hashId"]]
        _tmp["mc"] = _tmp["calendarDate"].apply(get_mc_factor)
        _tmp = _tmp.reset_index(drop=True)
        market_data.append(_tmp)


def optimize_price(dat, choice = 1):
    m = dat.copy()
    m['price'] = m.price.astype(str)
    m['price'] = m['price'].str.replace('$', '')
    m['price'] = m['price'].str.replace(',', '')
    mat =  m.iloc[:,:10].values.astype(float)
    dynasaur = PriceModel(market_matrix = mat, coeff = [-0.0062, 0.0003, 0.5, 0.1106, 0.3239, -0.015, 0.0002, 0.011, 0.42, 0.141], mc = choice)
    res = dynasaur.optimize()
    m["Optimized_Price"] = 0
    i = 0  
    j = "Optimized_Price"
    m.at[i, j] = res[1]
    return m


optimized_data = []
report_date = (datetime.now() - timedelta(days=1))
report_date = report_date.strftime("%Y-%m-%d")
            

def process_market_data(args):
    m, rm, report_date, RMid = args
    m["ToOptimize"] = (m['id'].astype(str) == str(rm._id)).astype(int)
    m = m.query('available == True or ToOptimize == 1')
    m = m.sort_values(by='ToOptimize', ascending=False)
    to_optimize = (m['ToOptimize'] == 1).any()
    num_comp = m.shape[0]
    if to_optimize and num_comp > 1:
        i = float(m.iloc[0]["mc"])
        optim = optimize_price(m, i)
        optim["report_date"] = report_date
        optim["ClientId"] = optim.at[0, "id"]
        optim["RMid"] = RMid
        return optim

def main(rental_market, market_data, report_date):
    # Preprocessing market_data
    market_data_processed = [df.drop_duplicates(subset=['id']) for df in market_data]

    # Prepare arguments for multiprocessing
    RMid = 1
    args = []
    for rm in rental_market:
        for m in market_data_processed:
            args.append((m, rm, report_date, RMid))
            RMid += 1

    # Use multiprocessing to process market data in parallel
    with Pool(processes=workers) as pool:  # Adjust the number of processes based on your system's capabilities
        optimized_data = pool.map(process_market_data, args)

    # Filter out None results if there's a chance process_market_data could return None
    optimized_data = [data for data in optimized_data if data is not None]

    return optimized_data

optimized_data = main(rental_market,market_data,report_date)
optimized_pricing = pd.concat(optimized_data,axis=0, ignore_index=True)
push_report(optimized_pricing)





client_property_data = pd.DataFrame(client_property_data)
client_property_data = client_property_data[["listing_id","user_id","_id"]]
client_property_data["listing_id"] = client_property_data.listing_id.astype(str)
client_property_data.columns = ["id","user_id","hashId"]

optimized_pricing["id"] = optimized_pricing.id.astype(str)
optimized_pricing["ClientId"] = optimized_pricing.id.astype(str)
optimized_pricing = optimized_pricing.query('id == ClientId and Optimized_Price > 0')
optimized_pricing = pd.merge(optimized_pricing,client_property_data,how = "left",on="id")
result = optimized_pricing.groupby(['hashId', 'user_id', 'listing_hashId'])[["calendarDate","Optimized_Price","price"]].agg(list).reset_index()
result_list = result.to_dict(orient='records')
formatted_data_list = [format_data(item) for item in result_list]
        
     
for f in formatted_data_list:
    push_data(f)