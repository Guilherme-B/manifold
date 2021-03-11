import json
import re

from datetime import datetime
from typing import Any, Dict, List, Tuple, Union, Optional

import scrapy

from bs4 import BeautifulSoup, SoupStrainer
from assets.items import ListingItem


class PTCentury21Spider(scrapy.Spider):

    name = 'pt_century21'
    allowed_domains = ['www.century21.pt']
    
    # override per scrapper configs
    custom_settings = {
      #'FEED_URI' : '%(category)s_%(subcategory)s.json'
     }

    __website: str = 'https://www.century21.pt'
    __api_endpoint: str = '/umbraco/Surface/C21PropertiesSearchListingSurface/GetAllSEO'

    # Broker info
    __country = "Portugal"
    __broker_name = "Century21"

    # Configuration
    __start_page: int = 1
    __items_per_page: int = 12
    __district: str = None
    __headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36',
        'host': 'www.century21.pt',
    }
    
    # maps the API's JSON structure to the ListingItem's
    __map_json: Dict[str, str] = {
        'ContractNumber': 'id',
        'PropertyType': 'property_type',
        'PriceCurrencyFormated': 'asking_price',
        'Bedrooms': 'bedrooms',
        'Bathrooms': 'bathrooms',
        'AreaGross': 'gross_area',
        'AreaNet': 'net_area',
        'Photo': 'avatar_url',
        'URLSEOv2': 'listing_url',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'Title': 'name'
    }

    def __init__(self, *args, **kwargs):
        super(PTCentury21Spider, self).__init__(*args, **kwargs)
        
        district: str = getattr(self,'district', None)
        
        if district:
            self.__district = district.strip()

    def start_requests(self):
        yield scrapy.Request(
            url=self._generate_url(page_number= self.__start_page),
            callback=self.parse,
            headers=self.__headers,
            dont_filter=False,
        )

    def parse(self, response: scrapy.http.TextResponse):
        data_json: Dict = json.loads(response.body.decode("utf-8"))
        listings_json: Dict[str, Any] = {}
        
        try:
            listings_json = data_json.get('Properties')
        except:
            listings_json = None
            
        if listings_json:
            for listing_json in listings_json:
                # Maps each JSON key to the corresponding ListingItem key through PTCentury21Spider.__map_json
                mapped_json: Dict[str, Any] = {self.__map_json.get(api_key): api_value for api_key, api_value in listing_json.items() if self.__map_json.get(api_key) is not None}

                # use the mapped data to initialize a ListingItem instance
                listing: ListingItem = ListingItem.from_json(mapped_json)

                # fix the URL to provide the complete path
                listing['listing_url'] = self.__website + '/' + listing['listing_url']
                
                # inject the broker data and country in the listing instance
                listing['broker'] = self.__broker_name
                listing['country'] = self.__country

                # scrape the listing's page for additional details
                if listing:
                    yield scrapy.Request(
                        url = listing['listing_url'],
                        callback = self._parse_detail,
                        dont_filter = False,
                        headers = 
                        {
                            'user-agent': self.__headers.get('user-agent'),
                            'origin': self.__website,
                        },
                        meta = {
                            'listing': listing,
                            'location': listing_json.get('FullLocation')
                        }
                    )
                    
            # get the total number of pages
            num_pages: int = data_json.get('TotalPages')
            # get the current page number
            current_page: int = data_json.get('CurrentPage')

            if num_pages and current_page < num_pages:
                yield scrapy.Request(
                    url=self._generate_url(page_number= current_page + 1),
                    callback=self.parse,
                    headers=self.__headers,
                    dont_filter=False,
                )
            
    def _parse_detail(self, response: scrapy.http.TextResponse): 
        listing: ListingItem = response.meta.get('listing')
        location: str = response.meta.get('location')
        
        if response is None:
            print(__name__, '::_parse_detail() could not retrieve listing instance')
            yield None
            
        if location is None:
            print(__name__, '::_parse_detail() could not retrieve location string')
            yield None
        
        '''
        listing_html = BeautifulSoup(
            response.body.decode("utf-8"), "html.parser")
        '''
        
        # parse the search page using SoupStrainer to limit the parsed data and lxml
        strainer = SoupStrainer(name = ['div', 'ul', 'li'])
        listing_html = BeautifulSoup(response.body, 'lxml', parse_only=strainer)
        
        if listing_html:
            self._extract_details(listing_html= listing_html, listing= listing)
            self._extract_administrative_data(location= location, listing= listing)
            
        yield listing
        
        
    def _extract_administrative_data(self, location: str, listing: ListingItem) -> None:
        if not location or not listing:
            return
        
        # The string location is in the format (Country, District, County, Parish, City)
        # e.g. Portugal, Coimbra, Figueira da Foz, Vila Verde, Vila Verde
        location_tokens: List[str] = location.split(',')
        tokens_num: int = len(location_tokens)

        if tokens_num > 2:
            listing['district'] = location_tokens[1] 
            if tokens_num > 3:  
                listing['county'] = location_tokens[2] 
                if tokens_num > 4:
                    listing['parish'] = location_tokens[3] 
                    
    def _extract_details(self, listing_html: str, listing: ListingItem) -> None:
        
        # the class common to all the details divs
        details_div_class: str = 'property-details-list'
        
        # the <ul> class for the property's details (price, areas, etc)
        property_detail_ul_class: str = 'caret-list multi-columns'
        
        # the <ul> class for the property's division details (kitchen sqrm, etc)
        division_detail_ul_class: str = 'multi-columns m0'
        
        # the <ul> class for the property's ammenities listing
        ammenity_detail_ul_class: str = 'tags-list'
        
        # extract all details divs, there can be up to three: asset; division; and ammenities detail 
        details_divs: str = listing_html.findAll(name='div', class_= details_div_class)

        if details_divs:
            for detail_div in details_divs:
                if detail_div.find(name= 'ul', class_= property_detail_ul_class):
                    
                    ### Parse the Transaction Type ###
                    
                    transaction_type_html: str = detail_div.find(text= re.compile(r'^Estado:*'))

                    if transaction_type_html:
                        listing['listing_type'] = transaction_type_html.find_parent('li').find('strong').string
                        
                    ### Parse the Energy Certification Information ###
                    
                    
                    energy_certificate_html: str = detail_div.find(text= re.compile(r'^Certificado energético:*'))

                    if energy_certificate_html:
                        energy_certificate_html = energy_certificate_html.find_parent('li').find('strong')
                        
                        if energy_certificate_html.find_parent('li').find('strong'):                            
                            listing['energy_certificate'] = energy_certificate_html.string
      
                
                    ### Parse Parking Information ###
                    
                    #garage_html: str = detail_div.find(lambda tag: tag.name == 'li' and tag.has_attr('string') and 'Tipo de Estacionamento' in tag['string'])
                    garage_html: str = detail_div.find(text= re.compile(r'^Tipo de estacionamento:*'))
                    
                    if garage_html:
                        # the garage definition is encapsulated in <strong> tags
                        garage_str: str = garage_html.find_parent('li').find('strong')
                        
                        if garage_str:
                            garage_str = garage_str.string
                            
                            # parse the garage number, there can be multiple occurences
                            # e.g. 2 Box Fechada
                            garage_tokens: List[str] = re.findall('\d', garage_str)
                            
                            if garage_tokens:
                                garage_num: int = sum([int(num) for num in garage_tokens])
                                listing['parking_spaces'] = garage_num
                                
                elif detail_div.find(name= 'ul', class_= division_detail_ul_class):
                    pass                                     
                elif detail_div.find(name= 'ul', class_= ammenity_detail_ul_class):
                    listing['ammenities'] = [amenity_li.string for amenity_li in detail_div.findAll('li')]
    
    def _generate_url(self, page_number: int) -> Union[str, None]:

        params: str = "?ord=date-desc"\
            "&page={page_number}"\
            "&numberOfElements={items_number}"\
            "&v=c"\
            "&ba="\
            "&be="\
            "&map="\
            "&mip="\
            "&q={district_query}"\
            "&ptd="\
            "&pstld="\
            "&mySite=False"\
            "&masterId=1"\
            "&seoId="\
            "&nodeId=46530"\
            "&language=pt-PT"\
            "&agencyId="\
            "&triggerbyAddressLocationLevelddl=false"\
            "&AgencySite_showAllAgenciesProperties=false"\
            "&AgencyExternalName="\
            "&b=2"\
            "&pt="\
            "&ls="\
            "&vt="\
            "&pstl="\
            "&cc="\
            "&et=".format(
                page_number=page_number,
                items_number=self.__items_per_page,
                district_query = 'portugal%2B' + self.__district if self.__district else ''
            )

        url: str = (self.__website +
                    self.__api_endpoint +
                    params)

        return url

