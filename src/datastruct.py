from dataclasses import dataclass, field
from typing import List

@dataclass
class ClientProperty:
    id: int = 0
    competitors: List[int] = field(default_factory=list)

@dataclass
class PropertyAttribute:
    price: float = 0.0
    review_count: int = 0
    image_score: float = 0.0
    bedroom: int = 0
    rating_value: float = 0.0
    min_stay: int = 0
    distance: float = 0.0
    pool: bool = False
    jacuzzi: bool = False
    landscape_views: bool = False
    id: int = 0
