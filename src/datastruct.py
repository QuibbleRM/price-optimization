class ClientProperty:
    
    _id = 0
    _competitors = []
    
    def __init__(self,id = 0, competitors = []):
        
        self._id = id
        self._competitors = competitors

class PropertyAttribute:

    
    _price = 0
    _review_count = 0
    _image_score = 0
    _bedroom = 0
    _rating_value = 0
    _min_stay = 0
    _distance = 0
    _pool = 0
    _jacuzzi = 0
    _landscape_views = 0
    _id = 0

    def __init__(self,price,review_count,image_score,bedroom,rating_value,min_stay,distance,pool,jacuzzi,landscape_views,id):

        self._price = price
        self._review_count = review_count
        self._image_score = image_score
        self._bedroom = bedroom
        self._rating_value = rating_value
        self._min_stay = min_stay
        self._distance = distance
        self._pool = pool
        self._jacuzzi = jacuzzi
        self._landscape_views = landscape_views
        self._id = id
        
    

