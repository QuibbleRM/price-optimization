import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import binom
from scipy.stats import norm
from .general import check_patterns_occurrence
from pymongo.collection import Collection

def get_image_scores(property_ids: list[str], image_scores_collection: Collection):
    score_query = [
                {
                    "$match": {"Listings": {"$in": property_ids}}
                },
                {
                    "$project": {
                        "_id": {"$toString": "$_id"},
                        "listings": "$Listings",
                        "file":  "$File",
                        "score": "$Score"
                        
                    }
                }
            ]
    
    return list(image_scores_collection.aggregate(score_query))

def get_listing_info(listing_ids: list[str], listings_collection: Collection):
    scrapy_query = [
        {
            "$match": {"id": {"$in": listing_ids}}
        },
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "id": "$id",
                "name": "$name",
                "description": "$description",
                "amenities": "$amenities",
                "accomodates": "$accomodates",
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

    return list(listings_collection.aggregate(scrapy_query))

def parse_scrap_info(scrap_dataframe: pd.DataFrame):
    scrape_list_df = scrap_dataframe
    scrape_list_df["pool"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["pool"], exact = True)
    scrape_list_df["jacuzzi"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["jacuzzi","hot tub","bathtub"])
    scrape_list_df["landscape_views"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["lake view","lake access","nature view","lake"])
    
    return scrape_list_df

def get_mc_factor(calendar_date: str):
    current_dir = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_dir)
    csv_file_path = os.path.join(parent_dir, 'files', 'bookable_search.csv')
    mc_factor = pd.read_csv(csv_file_path)

    date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%a")
    month = date_obj.strftime("%B")
    q = f'Month == "{month}" & Day == "{day_of_week}"'
    factor = mc_factor.query(q)
    
    return factor.Bookable_Search.iloc[0]

def get_property_info(property_ids: list[str], properties_collection: Collection):
    property_query = [
                {
                    "$match": {
                        "airBnbId": {"$in": property_ids},
                    }
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

    return list(properties_collection.aggregate(property_query))

def odd_weighted_average(row):
    values = list(row["scores"])
    
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

def get_availability_info(listing_ids: list[str], calendar_date: list[str], availability_collection: Collection, lag: int = 7):
    date_seven_days_ago = (datetime.now() - timedelta(days=lag))
   
    start_of_day = date_seven_days_ago.replace(hour=0, minute=0, second=0)
    end_of_day = datetime.now().replace(hour=23, minute=59, second=59)
    
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
            "$match": availability_match,
        },
        {
            "$sort": {"scraped_date": -1}  # Sort by scraped_date in descending order
        },
        {
            "$group": {
                "_id": "$listing_id",
                "latest_scraped": {"$first": "$$ROOT"}  # Keep the first document for each group (the latest one)
            }
        },
        {
            "$replaceRoot": {"newRoot": "$latest_scraped"}
        },
        {
            "$project": {
                "_id": 0,
                "id": "$listing_id",
                "calendar_date": "$calendarDate",
                "scraped_date": "$scraped_date",
                "available": "$available",
                "min_nights": "$minNights",
                "price": {"$ifNull": ["$price", 0]}
            }
        }
    ]

    matched_listings = list(availability_collection.aggregate(availability_query))
    matched_listing_ids = {listing['id'] for listing in matched_listings}

    for listing_id in listing_ids:
        if listing_id not in matched_listing_ids:
            matched_listings.append({
                "id": listing_id,
                "calendar_date": False,
                "scraped_date": False,
                "available": False,
                "min_nights": False,
                "price": False
            })
        
    return matched_listings