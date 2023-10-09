import pandas as pd
import numpy as np
from pymongo import MongoClient
from typing import Iterable, List
import re
import os
from datetime import datetime

clients = {
    "RevenueOS" : "mongodb+srv://qdev:ZHvklbN0wwakG396@qrm-production.ftpuq.mongodb.net",
    "MerlinHunter": "mongodb+srv://sguaro:jHdJU2TS2mckQZN2@merlinhunter.ftpuq.mongodb.net"
}

revenue_os = MongoClient(clients["RevenueOS"]) # properties
merlin_hunter = MongoClient(clients["MerlinHunter"]) # all properties, availability, information like bed #bedroom etc
def revenueos_connect():
    return MongoClient(clients["RevenueOS"])

def merlindb_connect():
    return MongoClient(clients["MerlinHunter"])

def get_property_info(property_ids: list[str]):
    
    #revenue_os = revenueos_connect()
    
    property_colllection = revenue_os["DB_quibble"]["properties"]
    
    property_match = {
            "airBnbId": {"$in": property_ids}
        } if property_ids else {}

    property_query = [
                {
                    "$match": property_match
                },
                {
                    "$project": {
                        "_id": 1,
                        "listing_id": "$airBnbId",
                        "virbo_id": "$virboId",
                        "intelCompSet": "$intelCompSet",
                        "id": "$id",
                        "user_id": "$userId",
                        "name": 1
                    }
                }
            ]
    
    properties: Iterable[dict] = property_colllection.aggregate(property_query)

    #revenue_os.close()
    
    property_list = []

    for _property in properties:

        if not _property.get('listing_id'):
            if not _property.get('virbo_id'):
                continue
            continue

        property_list.append(_property)
    
    return property_list

def get_listing_info(listing_ids: list[str]):
    
    #merlin_hunter = merlindb_connect()
    
    scrapy_collection  = merlin_hunter["scrapy_quibble"]["scrapy_listing"]

    scrapy_match = {
                "id": {"$in": listing_ids}
            } if listing_ids else {}


    scrapy_query = [
                {
                    "$match": scrapy_match
                },
                {
                    "$project": {
                        "_id": 0,
                        "id": "$id",
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
                        "rating_value": "$avg_rating",
                        "review_count": "$review_count",
                        "room_type": "$room_type"
                    }
                }

            ]

    scrapy_listings: Iterable[dict] = scrapy_collection.aggregate(scrapy_query)

    #merlin_hunter.close()
    
    scrapy_list = []

    for _list in scrapy_listings:

        if not _list.get('id'):
            continue

        scrapy_list.append(_list)
        
    return scrapy_list


def get_availability_info(listing_ids: list[str], calendar_date: list[str]):
    
    start_date = datetime.strptime("2023-09-21", "%Y-%m-%d") 
    
    start_of_day = start_date.replace(hour=0, minute=0, second=0)
    end_of_day = start_date.replace(hour=23, minute=59, second=59)

    availability_collection  = merlin_hunter["scrapy_quibble"]["scrapy_availability"]
    
    availability_match = {
            "listing_id": {"$in": listing_ids},
            "calendarDate": {"$in": calendar_date},
            "minNights": {"$lt": 30},
            "scraped_date": {
                    "$gte": start_of_day,
                    "$lt": end_of_day
                },
            "price": {"$exists": True} if listing_ids else None
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
                        "price": "$price"
                    }
                }

            ]

    availabilities: Iterable[dict] = availability_collection.aggregate(availability_query)

    #merlin_hunter.close()
    
    available_list = []
    
    for _avail in availabilities:

        if not _avail.get('id'):
            continue

        available_list.append(_avail)
        
    return available_list


def check_pattern_occurrence(arr,pattern):
    
    pattern_regex = re.compile(pattern, re.IGNORECASE)
    
    for item in arr:
        if pattern_regex.search(str(item)):
            return 1
    return 0


def check_patterns_occurrence(arr, patterns, exact = False):
    for pattern in patterns:
        pattern_regex = re.compile(pattern, re.IGNORECASE)
        for item in arr:
            if exact == False:
                if pattern_regex.search(str(item)):
                    return 1
            else:
                if pattern_regex.fullmatch(str(item)):
                    return 1
    return 0


def parse_scrap_info(scrap_dataframe):
    
    scrape_list_df = scrap_dataframe
    scrape_list_df["pool"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["pool"])
    scrape_list_df["jacuzzi"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["jacuzzi","hot tub","bathtub"])
    scrape_list_df["landscape_views"] = scrape_list_df['amenities'].apply(check_patterns_occurrence, patterns=["nature", "lake view","lake"])
    
    return scrape_list_df



from scipy.stats import binom


def odd_weighted_average(row):
    
    #print(image_scores)
    
    values = list(row["Scores"])
    
    #values = remove_zeroes(values)
    mean_score = np.mean(values)
    sd_score = np.std(values)
    
    # Calculate the total number of values
    total_values = len(values)

    # Create a list to store the results for each value
    results_list = []

    # Calculate the p-value and confidence interval for each value
    for value in values:
        value_count = sum(1 for x in values if x <= value)
        odds = value_count / total_values
        probability = norm.cdf(value, loc = mean_score, scale = sd_score)
        #odds = probability
        
        # Calculate the p-value using the binomial CDF
        p_value = 1 - binom.cdf(value_count - 1, total_values, odds)

        # Calculate the standard error for a binomial distribution
        std_error = (odds * (1 - odds) / total_values) ** 0.5

        # Calculate the z-score
        z_score = norm.ppf(1 - p_value / 2)

        # Calculate the confidence interval
        conf_interval = (odds - z_score * std_error, odds + z_score * std_error)

        results_list.append((value, odds))
    
    # Calculate the weighted average
    weighted_sum = sum(odd * value for value, odd in results_list)
    total_sum_of_odds = sum(odd for _, odd in results_list)
    weighted_average = weighted_sum / total_sum_of_odds
    
    return (mean_score,weighted_average,abs(mean_score - weighted_average))

from scipy.stats import binom
from scipy.stats import norm
import numpy as np

def prob_weighted_average(row):
    
    #values = list(image_scores[1:])
    
    #values = remove_zeroes(values)
    values = list(row["Scores"])
    mean_score = np.mean(values)
    sd_score = np.std(values)
    
    # Calculate the total number of values
    total_values = len(values)

    # Create a list to store the results for each value
    results_list = []

    # Calculate the p-value and confidence interval for each value
    for value in values:
        value_count = sum(1 for x in values if x <= value)
        odds = value_count / total_values
        probability = norm.cdf(value, loc = mean_score, scale = sd_score)
        #odds = probability
        
        # Calculate the p-value using the binomial CDF
        p_value = 1 - binom.cdf(value_count - 1, total_values, odds)

        # Calculate the standard error for a binomial distribution
        std_error = (odds * (1 - odds) / total_values) ** 0.5

        # Calculate the z-score
        z_score = norm.ppf(1 - p_value / 2)

        # Calculate the confidence interval
        conf_interval = (odds - z_score * std_error, odds + z_score * std_error)

        results_list.append((value, probability))
    
    # Calculate the weighted average
    weighted_sum = sum(prob * value for value, prob in results_list)
    total_sum_of_prob = sum(prob for _, prob in results_list)
    weighted_average = weighted_sum / total_sum_of_prob
    
    return (mean_score,weighted_average,abs(mean_score - weighted_average))

def get_image_scores(property_ids: list[str]):
    
   
    
    score_collection = merlin_hunter["scrapy_quibble"]["scrapy_image_scores"]
    
    score_match = {
            "Listings": {"$in": property_ids}
        } if property_ids else {}

    score_query = [
                {
                    "$match": score_match
                },
                {
                    "$project": {
                        "Listings": "$Listings",
                        "Score": "$Score"
                        
                    }
                }
            ]
    
    scores: Iterable[dict] = score_collection.aggregate(score_query)

    #revenue_os.close()
    
    score_list = []

    for _score in scores:

        if not _score.get('Listings'):
            continue

        score_list.append(_score)
    
    return score_list


def get_image_scores(property_ids: list[str]):
    
    score_collection = merlin_hunter["scrapy_quibble"]["scrapy_image_scores"]
    
    score_match = {
            "Listings": {"$in": property_ids}
        } if property_ids else {}

    score_query = [
                {
                    "$match": score_match
                },
                {
                    "$project": {
                        "Listings": "$Listings",
                        "Score": "$Score"
                        
                    }
                }
            ]
    
    scores: Iterable[dict] = score_collection.aggregate(score_query)

    #revenue_os.close()
    
    score_list = []

    for _score in scores:

        if not _score.get('Listings'):
            continue

        score_list.append(_score)
    
    return score_list


def format_data(input_data):
    today = datetime.today()
    extracted_data = input_data
    formatted_data = {
        'property_id': extracted_data['hashId'],
        'calendar': {
            today.strftime("%Y-%m-%d"): {
                'optimized_price': {
                    d: op_price
                    for d, op_price in zip(
                        extracted_data['calendarDate'],
                        extracted_data['Optimized_Price']
                    )
                },
                'price': {
                    d: p
                    for d, p in zip(
                        extracted_data['calendarDate'],
                        extracted_data['price']
                    )
                }
            
            }
        },
        'listing_id': extracted_data['id'],
        'user_id': extracted_data['user_id']
        
    }
    return formatted_data