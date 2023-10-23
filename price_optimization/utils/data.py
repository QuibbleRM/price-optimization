import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import binom
from scipy.stats import norm
from .general import check_patterns_occurrence
from pymongo.collection import Collection

def get_image_scores(property_ids: list[str], image_scores_collection: Collection):

    if property_ids:
        score_match = {
            "Listings": {"$in": property_ids}
        }
    else:
        score_query = [
                    {
                        "$match": score_match
                    },
                    {
                        "$project": {
                            "_id": {"$toString": "$_id"},
                            "Listings": "$Listings",
                            "Score": "$Score"
                            
                        }
                    }
                ]
    
    scores = image_scores_collection.aggregate(score_query)

    score_list = []
    for _score in scores:
        if not _score.get('Listings'):
            continue
        score_list.append(_score)
    
    return score_list

def get_listing_info(listing_ids: list[str], listings_collection: Collection):
    if listing_ids:
        scrapy_match = {"id": {"$in": listing_ids}}
    else:
        scrapy_match = {}

    scrapy_query = [
        {
            "$match": scrapy_match
        },
        {
            "$project": {
                "_id": 0,
                "id": "$id",
                "amenities": "$amenities",
                "accommodates": "$accomodates",
                "bathrooms": "$bathrooms",
                "bedrooms": "$bedrooms",
                "beds": "$beds",
                "city": "$city",
                "state": "$state",
                "country": "$country",
                "neighbourhood": "$neighborhood",
                "host_name": "$host_name",
                "rating_value": {
                    "$ifNull": ["$avg_rating", 0]
                },
                "review_count": {
                    "$ifNull": ["$review_count", 0]
                },
                "room_type": "$room_type"
            }
        }
    ]

    scrapy_listings = list(listings_collection.aggregate(scrapy_query))
    scrapy_list = []
    for _list in scrapy_listings:
        if not _list.get('id'):
            continue
        scrapy_list.append(_list)

    return scrapy_list

def parse_scrap_info(scrap_dataframe: pd.DataFrame):
    scrape_list_df = scrap_dataframe
    scrape_list_df["pool"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["pool"], exact = True)
    scrape_list_df["jacuzzi"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["jacuzzi","hot tub","bathtub"])
    scrape_list_df["landscape_views"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["lake view","lake access","nature view","lake"])
    
    return scrape_list_df

def get_mc_factor(calendar_date: str):
    mc_factor = pd.read_csv("files/bookable_search.csv")
    date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%a")
    month = date_obj.strftime("%B")
    q = f'Month == "{month}" & Day == "{day_of_week}"'
    factor = mc_factor.query(q)
    
    return factor.Bookable_Search.iloc[0]

def get_property_info(property_ids: list[str], properties_collection: Collection):
    if property_ids:
        property_match = {
            "airBnbId": {"$in": property_ids}
        }
    else:
        property_match = {}

    property_query = [
                {
                    "$match": property_match
                },
                {
                    "$project": {
                        "_id": {"$toString": "$_id"},
                        "listing_id": "$airBnbId",
                        "virbo_id": "$virboId",
                        "intelCompSet": "$intelCompSet",
                        "id": "$id",
                        "user_id": {"$toString": "$userId"},
                        "name": 1
                    }
                }
            ]
    
    properties = properties_collection.aggregate(property_query)


    property_list = []
    for _property in properties:
        if not _property.get('listing_id'):
            if not _property.get('virbo_id'):
                continue
            continue

        property_list.append(_property)
    
    return property_list


def odd_weighted_average(row):
    values = list(row["Scores"])
    
    mean_score = np.mean(values)
    sd_score = np.std(values)
    total_values = len(values)
    results_list = []

    for value in values:
        value_count = sum(1 for x in values if x <= value)
        odds = value_count / total_values
        probability = norm.cdf(value, loc = mean_score, scale = sd_score)
        p_value = 1 - binom.cdf(value_count - 1, total_values, odds)
        std_error = (odds * (1 - odds) / total_values) ** 0.5
        z_score = norm.ppf(1 - p_value / 2)
        conf_interval = (odds - z_score * std_error, odds + z_score * std_error)

        results_list.append((value, odds))
    
    weighted_sum = sum(odd * value for value, odd in results_list)
    total_sum_of_odds = sum(odd for _, odd in results_list)
    weighted_average = weighted_sum / total_sum_of_odds
    
    return (mean_score,weighted_average,abs(mean_score - weighted_average))


def get_availability_info(listing_ids: list[str], calendar_date: list[str], availability_collection: Collection, lag:int = 1):
    date_yesterday = (datetime.now() - timedelta(days=lag))
   
    start_of_day = date_yesterday.replace(hour=0, minute=0, second=0)
    end_of_day = date_yesterday.replace(hour=23, minute=59, second=59)

    availability_match = {
            "listing_id": {"$in": listing_ids},
            "calendarDate": {"$in": calendar_date},
            "minNights": {"$lt": 30},
            "scraped_date": {
                    "$gte": start_of_day,
                    "$lt": end_of_day
                }
        }
  
    availability_query = [
                {
                    "$match": availability_match
                },
                {
                    "$project": {
                        "_id": 0,
                        "id": "$listing_id",
                        "calendarDate": "$calendarDate",
                        "scraped_date": "$scraped_date",
                        "available": "$available",
                        "minNights": "$minNights",
                        "price": { "$ifNull" : [ "$price", 0 ] }
                    }
                }

            ]
    
    availabilities = availability_collection.aggregate(availability_query)

    available_list = []
    for _avail in availabilities:
        if not _avail.get('id'):
            continue
        
        available_list.append(_avail)
        
    return available_list
    
