import os
import re
from .dynapee import PriceModel
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from .data_struct import ClientProperty, PropertyAttribute
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
    COEFFICIENTS = [-0.0062, 0.0003, 0.0879, 0.1106, 0.3239, 0.015, 0.0002, 0.011, 0.42, 0.141]
    PROPERTY_ATTRS_COLUMNS = [
            "price",
            "review_count",
            "adjusted",
            "bedrooms",
            "rating_value",
            "min_nights",
            "dist",
            "pool",
            "jacuzzi",
            "landscape_views"
        ]

    def __init__(self, property_info: PropertyAttribute, calendar_date: str):
        self.property_info = property_info
        self.calendar_date = calendar_date

        self._setup_file_paths()
        self._setup_database_connections()

        client_property_data = get_property_info([self.property_info.id], self.revOS['DB_quibble']['properties'])[0]
        self.rental_market = ClientProperty(id=client_property_data["listing_id"], competitors=client_property_data["intelCompSet"])

        self.all_ids = list(set([client_property_data["listing_id"]] + client_property_data["intelCompSet"]))

    def _setup_file_paths(self) -> None:
        current_dir = os.path.dirname(__file__)
        csv_file_path = os.path.join(current_dir, 'files', 'bookable_search.csv')
        self.mc_factor = pd.read_csv(csv_file_path)
        self.image_base_url = "https://qrm-listing-images.s3.amazonaws.com/airbnb"

    def _setup_database_connections(self) -> None:
        self.revOS = MongoClient(os.getenv('MONGO_REVENUE_OS_URI'), socketTimeoutMS=1800000, connectTimeoutMS=1800000)
        self.merlinHunter = MongoClient(os.getenv('MONGO_MERLIN_HUNTER_URI'), socketTimeoutMS=1800000, connectTimeoutMS=1800000)

    def _query_mc_factor(self, calendar_date: str) -> float:
        date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
        query = f'Month == "{date_obj.strftime("%B")}" & Day == "{date_obj.strftime("%a")}"'
        factor = self.mc_factor.query(query)
        return factor.Bookable_Search.iloc[0]

    def _format_price(self, data: pd.DataFrame) -> pd.DataFrame:
        data['price'] = data['price'].apply(
            lambda x: re.sub(r'[$,]', '', x) if isinstance(x, str) else x
        ).astype(float)

        return data

    def _aggregate_image_scores(self, image_set):
        image_scores = image_set.groupby('listings')['score'].agg(list).reset_index()
        image_scores.columns = ["id", "scores"]
        image_scores["values"] = image_scores.apply(odd_weighted_average, axis=1)
        image_scores[['reference', 'adjusted', 'factor']] = image_scores['values'].apply(lambda x: pd.Series(x))
        image_scores = image_scores[["id", "scores", "reference", "adjusted"]]

        return image_scores

    def _get_listing_data(self, image_set, market_availabilities):
        image_scores = self._aggregate_image_scores(image_set)

        client_listing = pd.DataFrame(get_listing_info([self.rental_market.id], self.merlinHunter["scrapy_quibble"]["scrapy_listing"]))
        competitor_listing = pd.DataFrame(get_listing_info(self.rental_market.competitors, self.merlinHunter["scrapy_quibble"]["scrapy_listing"]))

        market_listing = pd.concat([client_listing, competitor_listing], axis=0)
        market_listing["bedrooms"] = pd.to_numeric(market_listing["bedrooms"], errors="coerce").fillna(0).astype(int)
        market_listing.rename(columns={'_id': 'listing_hash_id'}, inplace=True)
        market_listing = parse_scrap_info(market_listing)

        market_listing = pd.merge(market_listing, image_scores, on="id", how="outer")
        market_listing["reference"].fillna(market_listing["reference"].mean(), inplace=True)
        market_listing["adjusted"].fillna(market_listing["adjusted"].mean(), inplace=True)
        
        market_listing = pd.merge(market_listing, market_availabilities, on="id", how='inner')
        market_listing['dist'] = 0

        market_listing = market_listing[[ "id", "name", "description", *self.PROPERTY_ATTRS_COLUMNS, "available", "calendar_date", "listing_hash_id"]]
        market_listing["mc"] = market_listing["calendar_date"].apply(
            lambda x: get_mc_factor(x) if isinstance(x, str) else None
        )
        
        market_listing = market_listing.drop_duplicates(subset=['id'])
        market_listing["to_optimize"] = market_listing['id'].apply(lambda x: 1 if str(x) == str(self.rental_market.id) else 0)

        market_listing = market_listing.sort_values(by='to_optimize', ascending=False)

        return market_listing

    def _calculate_metric(self, data: pd.DataFrame, choice: int, metric: str) -> pd.DataFrame:
        processed_data = self._format_price(data)

        filtered_data = processed_data[((processed_data['available'] != True) | (processed_data['price'] <= 0)) & (processed_data['to_optimize'] != 1)]
        processed_data = processed_data.drop(filtered_data.index)

        matrix = processed_data[self.PROPERTY_ATTRS_COLUMNS].values.astype(float)
        price_model = PriceModel(market_matrix=matrix, coeff=self.COEFFICIENTS, mc=choice)

        if metric not in ["price", "share"]:
            raise ValueError(f"Invalid metric: {metric}")

        if metric == "price":
            result = price_model.optimize()
            processed_data["optimized_price"] = result[1]
        elif metric == "share":
            result = price_model.compute_share()
            processed_data["market_share"] = result

        merged_data = pd.concat([processed_data, filtered_data]
        merged_data.loc[(merged_data['available'] != True) & (merged_data['to_optimize'] != 1), 'price'] = None

        return merged_data

    def _compute_share(self, image_set, market_availabilities):
        columns = [
            "id", "name", "description", "price", "review_count", "adjusted", "bedrooms",
            "rating_value", "min_nights", "dist", "pool", "jacuzzi", "landscape_views", 
            "available", "calendar_date", "listing_hash_id", "mc", "to_optimize"
        ]
        market_share = pd.DataFrame(columns=columns)
        market_listing_data = self._get_listing_data(image_set=image_set, market_availabilities=market_availabilities)
        
        prop_info_dict = vars(self.property_info)
        prop_info_dict["adjusted"] = prop_info_dict.pop("image_score")
        market_listing_data.loc[0, prop_info_dict.keys()] = prop_info_dict.values()

        to_optimize = (market_listing_data['to_optimize'] == 1).any()
        num_comp = market_listing_data.shape[0]

        if to_optimize and num_comp > 1:
            i = float(market_listing_data.iloc[0]["mc"])
            market_share = self._calculate_metric(market_listing_data, i, "share")

        return market_share

    def get_market_data(self):
        image_set = get_image_scores(self.all_ids, self.merlinHunter["scrapy_quibble"]["scrapy_image_scores"])
        image_set = pd.DataFrame(image_set)
        image_set["image_url"] = self.image_base_url + "/" + image_set['listings'] + "/" + image_set['file']

        market_availabilities = pd.DataFrame(get_availability_info(self.all_ids, [self.calendar_date], self.merlinHunter["scrapy_quibble"]["scrapy_availability"]))

        market_share = self._compute_share(image_set=image_set, market_availabilities=market_availabilities)

        return market_share, image_set, market_availabilities

    def optimize_price(self):
        image_set = get_image_scores(self.all_ids, self.merlinHunter["scrapy_quibble"]["scrapy_image_scores"])
        image_set = pd.DataFrame(image_set)

        market_availabilities = pd.DataFrame(get_availability_info(self.all_ids, [self.calendar_date], self.merlinHunter["scrapy_quibble"]["scrapy_availability"]))

        optimized_price = None
        market_listing_data = self._get_listing_data(image_set=image_set, market_availabilities=market_availabilities)
        to_optimize = (market_listing_data['to_optimize'] == 1).any()
        num_comp = market_listing_data.shape[0]

        if to_optimize and num_comp > 1:
            i = float(market_listing_data.iloc[0]["mc"])
            optimized_price = self._calculate_metric(market_listing_data, i, "price")

        return optimized_price