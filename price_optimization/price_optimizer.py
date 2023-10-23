import os
from .dynapee import PriceModel
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from .data_struct import ClientProperty, PropertyAttribute
from .utils.general import instance_to_array
from .utils.data import (
    get_image_scores, 
    get_listing_info, 
    parse_scrap_info, 
    get_mc_factor, 
    get_property_info, 
    odd_weighted_average, 
    get_availability_info
)

class PriceOptimizer:
    def __init__(self):
        self.mc_factor = pd.read_csv("./files/bookable_search.csv")

    def get_mc_factor(self, calendar_date: str) -> float:
        date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
        query = f'Month == "{date_obj.strftime("%B")}" & Day == "{date_obj.strftime("%a")}"'
        factor = self.mc_factor.query(query)
        return factor.Bookable_Search.iloc[0]

    def process_market_data(self, data: pd.DataFrame) -> pd.DataFrame:
        processed_data = data.copy()
        processed_data['price'] = processed_data['price'].str.replace('[$,]', '', regex=True).astype(float)
        return processed_data

    def calculate_metric(self, data: pd.DataFrame, choice: int, metric: str) -> pd.DataFrame:
        processed_data = self.process_market_data(data)
        matrix = processed_data.iloc[:, :10].values.astype(float)
        price_model = PriceModel(market_matrix=matrix, coeff=[-0.0062, 0.0003, 0.0879, 0.1106, 0.3239, 0.015, 0.0002, 0.011, 0.42, 0.141], mc=choice)

        if metric == "price":
            result = price_model.optimize()
            processed_data["Optimized_Price"] = result[1]
        elif metric == "share":
            result = price_model.compute_share()
            processed_data["Market_Share"] = result

        return processed_data

    def build_matrix(self,listing_id: str, calendar_date: str):
        revOS = MongoClient(os.getenv('MONGO_REVENUE_OS_URI'), socketTimeoutMS=1800000, connectTimeoutMS=1800000) 
        merlinHunter = MongoClient(os.getenv('MONGO_MERLIN_HUNTER_URI'), socketTimeoutMS=1800000, connectTimeoutMS=1800000)

        client_property_data = get_property_info([listing_id], revOS['DB_quibble'])[0]
        rental_market = ClientProperty(id = client_property_data["listing_id"],competitors = client_property_data["intelCompSet"])

        all_ids = []
        all_ids.append(client_property_data["listing_id"])
        all_ids.extend(client_property_data["intelCompSet"])
        all_ids = list(set(all_ids))

        image_set = get_image_scores(all_ids, merlinHunter["scrapy_quibble"]["scrapy_image_scores"])
        image_set = pd.DataFrame(image_set)
        image_scores = image_set.groupby('Listings')['Score'].agg(list).reset_index()
        image_scores.columns = ["id","Scores"]
        image_scores["Values"] = image_scores.apply(odd_weighted_average, axis=1)
        image_scores[['Reference', 'Adjusted', 'Factor']] = image_scores['Values'].apply(lambda x: pd.Series(x))
        image_scores = image_scores[["id","Scores","Reference","Adjusted"]]

        comp_list = []
        [comp_list.append(x) for x in rental_market._competitors]
                
        client_listing = pd.DataFrame(get_listing_info([listing_id], merlinHunter["scrapy_quibble"]["scrapy_image_scores"]))
        competitor_listing = pd.DataFrame(get_listing_info(comp_list, merlinHunter["scrapy_quibble"]["scrapy_image_scores"]))
        market_listing = pd.concat([client_listing,competitor_listing],axis = 0)
        market_listing["bedrooms"] = pd.to_numeric(market_listing["bedrooms"], errors="coerce").fillna(0).astype(int)
        market_listing.rename(columns={'_id': 'listing_hashId'}, inplace=True)
        market_listing = parse_scrap_info(market_listing)
        market_listing = pd.merge(market_listing,image_scores, on = "id", how = "outer")
        market_listing["Reference"].fillna(market_listing["Reference"].mean(),inplace = True)
        market_listing["Adjusted"].fillna(market_listing["Adjusted"].mean(),inplace = True)

        market_availabilities = pd.DataFrame(get_availability_info(all_ids,[calendar_date], merlinHunter["scrapy_quibble"]["scrapy_availability"]))
        market_listing= pd.merge(market_listing,market_availabilities,on="id", how = 'inner')
        market_listing['dist'] = 0

        market_data = market_listing[["price","review_count","Adjusted","bedrooms","rating_value","minNights","dist","pool","jacuzzi","landscape_views","id","available","calendarDate","listing_hashId"]]
        market_data["mc"] = market_data["calendarDate"].apply(get_mc_factor)
        
        return market_data

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
            optimized_price = self.calculate_metric(market_data,i,"price")
        
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
            market_share = self.calculate_metric(market_data,i,"share")

        return market_share





