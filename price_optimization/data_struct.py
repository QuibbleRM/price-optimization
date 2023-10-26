class ClientProperty:
    id = 0
    competitors = []
    
    def __init__(self, id=0, competitors=[]):
        self.id = id
        self.competitors = competitors


class PropertyAttribute:
    price = 0
    review_count = 0
    image_score = 0
    bedroom = 0
    rating_value = 0
    min_stay = 0
    distance = 0
    pool = 0
    jacuzzi = 0
    landscape_views = 0
    id = 0

    def __init__(self,price,review_count,image_score,bedroom,rating_value,min_stay,distance,pool,jacuzzi,landscape_views,id):
        self.price = price
        self.review_count = review_count
        self.image_score = image_score
        self.bedroom = bedroom
        self.rating_value = rating_value
        self.min_stay = min_stay
        self.distance = distance
        self.pool = pool
        self.jacuzzi = jacuzzi
        self.landscape_views = landscape_views
        self.id = id
        
    

