from typing import Any, Dict, List

import scrapy

from itemloaders.processors import MapCompose

def remove_unicode(value: str) -> str:
    return value.replace(u"\u201c", '').replace(u"\u201d", '').replace(u"\2764", '').replace(u"\ufe0f", '')


class ListingItem(scrapy.Item):

    # Listing Header
    id = scrapy.Field()
    broker = scrapy.Field()
    name = scrapy.Field(input_processor = MapCompose(remove_unicode))
    summary = scrapy.Field(input_processor = MapCompose(remove_unicode))
    description = scrapy.Field(input_processor = MapCompose(remove_unicode))
    country = scrapy.Field()
    district = scrapy.Field()
    county = scrapy.Field()
    parish = scrapy.Field()
    city = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    
    asking_price = scrapy.Field()
    
    # The listing's main photo
    avatar_url = scrapy.Field()
    
    # Rental, Sale, etc
    listing_type = scrapy.Field()
    property_type = scrapy.Field()
    listing_url = scrapy.Field()
    listing_photos = scrapy.Field()
        
    # Listing Documentation
    gross_area = scrapy.Field()
    net_area = scrapy.Field()
    energy_certificate = scrapy.Field()
    
    # Listing Composition
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    parking_spaces = scrapy.Field()
    ammenities = scrapy.Field()

    @classmethod
    def from_json(cls, json_str: Dict):
        instance = cls(json_str)

        return instance


    def to_list(self) -> List[Any]:
        return self.values()