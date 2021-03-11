# Python imports
import re

from datetime import datetime
from typing import Callable, Dict, List, Tuple, Union, Optional

import scrapy

from bs4 import BeautifulSoup
from assets.items import ListingItem


class PTEraSpider(scrapy.Spider):

    name = 'pt_era'
    allowed_domains = ['www.era.pt']
    
    # override per scrapper configs
    custom_settings = {
      #'FEED_URI' : '%(category)s_%(subcategory)s.json'
     }

    __website: str = 'https://www.era.pt'
    __api_endpoint: str = '/properties/'

    # Broker info
    __country = "Portugal"
    __broker_name = "ERA Imobiliária"

    # Configuration
    __start_page: int = 1
    __headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36',
        'referer': __website,
    }

    # maps the ListingItem variable to the corresponding HTML extraction configuration
    # Note: the same could be achieved with lambda functions, which are faster given they don't require unpacking
    # however, the performance impact is small, and readability increases drastically, and allows for dynamic configs
    __data_map: Dict[str, Callable[..., str]] = {
        'id': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_ref'},
        'listing_type': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_finalidade'},
        'property_type': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_tipo_imovel'},
        'asking_price': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_preco_venda'},
        'district': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_distrito'},
        'county': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_concelho'},
        'parish': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_freguesia'},
        'city': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_distrito'},
        'gross_area': {'name': 'span', 'id': 'ctl00_ContentPlaceHolder1_lbl_imovel_show_area_bruta'},

        'listing_url': {'name': 'meta', 'property': 'og:url'},
        'summary': {'name': 'meta', 'property': 'og:title'},
        'avatar_url': {'name': 'meta', 'property': 'og:image'},

    }

    __header_map: Dict[str, str] = {
        "Bedrooms": "bedrooms",
        "Bathroom": "bathrooms",
        "Parking": "parking_spaces",
        "Energy_Certificate": "energy_certificate"
    }

    def __init__(self, *args, **kwargs):
        super(PTEraSpider, self).__init__(*args, **kwargs)
        
        destination_s3: str = kwargs.pop('s3_location', None)
        
        if destination_s3:
            current_time: datetime = datetime.now()
            
            current_date: datetime = datetime.today()
            year: int = current_time.year
            month: int = current_time.month
            day: int = current_time.day
            
            destination_s3.format(
                date = current_date,
                year = year,
                month = month,
                day = day,
                country = self.__country,
                broker = self.__broker_name
            )
            
            self.custom_settings['FEED_URI'] = destination_s3
            

    def start_requests(self):

        yield scrapy.Request(
            url=self._generate_url(page_number=self.__start_page),
            callback=self.parse,
            headers=self.__headers,
            dont_filter=False,
        )

    def parse(self, response: scrapy.http.TextResponse):
        html_data = BeautifulSoup(response.body.decode("utf-8"), "html.parser")

        if html_data:
            # retrieve the table containing the listing's header
            listings_table = html_data.find(
                name='table', id='ctl00_ContentPlaceHolder1_DataList_imoveis')

            # retrieve the individual listing's header
            listing_rows = listings_table.findAll(
                lambda tag: tag.name == 'a' and tag.has_attr('id'))

            # retrieve each link and request its detail
            for listing in listing_rows:

                # get the listing's individual URL
                listing_url: str = listing.get('href')

                if listing_url:
                    yield scrapy.Request(
                        url=(self.__website + listing_url),
                        callback=self._parse_detail,
                        dont_filter=False,
                        headers={
                            'user-agent': self.__headers.get('user-agent'),
                            'origin': self.__website,
                        }
                    )

            # assert the existance of a new page by checking if the next page button is active
            pagination_metadata: str = html_data.find(
                name='div', id='ctl00_ContentPlaceHolder1_pnl_paginacao_setas2')

            # if a link is present, there is a next page
            next_page: str = pagination_metadata.find(
                lambda tag: tag.name == 'a' and tag.has_attr('href')).get('href')

            if next_page:
                url: str = (self.__website +
                            next_page)

                yield scrapy.Request(
                    url=url,
                    headers=self.__headers,
                    dont_filter=False
                )

    def _generate_url(self, **kwargs) -> Union[str, None]:

        params: str = "?pg={page_number}".format(
            **kwargs
        )

        url: str = (self.__website +
                    self.__api_endpoint +
                    params)

        return url

    def _parse_detail(self, response: scrapy.http.TextResponse):

        listing_html = BeautifulSoup(
            response.body.decode("utf-8"), "html.parser")

        if listing_html:
            listing = ListingItem()

            listing['broker'] = self.__broker_name
            listing['country'] = self.__country

            '''
                Data Extraction Section
            '''

            # associate each ListingItem variable to the corresponding extracted data, if any
            self.__map_features(listing, listing_html, PTEraSpider.__data_map)

            # retrieve the base indicators present in the body's "header"
            self.__parse_indicators(listing, listing_html)

            # extract coordinates
            latitude, longitude = self.__extract_coordinates(listing_html)

            if latitude and longitude:
                listing['latitude'] = latitude
                listing['longitude'] = longitude

            yield listing

        yield None

    def __map_features(self, item: ListingItem, html_content: str, map: Dict[str, Callable[..., None]]) -> None:
        for variable, content in map.items():
            try:
                result: str = html_content.find(content)

                if result:
                    item[variable] = result.string
            except Exception as e:
                print('Failed to retrieve variable {variable} with map {content} , {e}'.format(
                    variable, content, e))

    def __parse_indicators(self, item: ListingItem, html: str) -> None:
        header_content = html.select('.bloco-caracteristicas')

        if header_content:
            icon_sections: List[str] = header_content[0].find_all('li')

            for icon_section in icon_sections:
                category: str = icon_section.find(
                    lambda tag: tag.name == 'span' and tag.has_attr('title'))
                value: str = icon_section.find('span', ['num'])

                # hardcoded, unfortunately this is an edge-case
                # if the num isn't found, then the field corresponds to an energy certificate
                if value is None:
                    value = category['title'].replace(
                        'Energ. Cert.:', '').strip()
                    category = 'Energy_Certificate'
                elif category and value:
                    category = category['title']
                    value = value.string

                if category and value:
                    # associate each ListingItem variable to the corresponding extracted data, if any
                    try:
                        listing_variable = PTEraSpider.__header_map.get(
                            category)

                        if listing_variable:
                            item[listing_variable] = value
                    except Exception as e:
                        print('Failed to retrieve variable {variable} with map {content} , {e}'.format(
                            category, value, e))

    def __extract_coordinates(self, html: str) -> Optional[Tuple[float, float]]:
        regex_pattern: str = 'query=[+-]?([0-9]*[.])?[0-9]+,[+-]?([0-9]*[.])?[0-9]+'

        link = html.find('img', ['img_mapa', 'onclick'])

        if link:
            try:
                google_query: str = link['onclick']

                coordinates: Tuple[float, float] = re.findall(
                    regex_pattern, google_query)

                if coordinates:
                    return coordinates[0]

            except Exception as e:
                return None, None
