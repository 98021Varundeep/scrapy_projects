
from typing import Iterable
import scrapy
from chocolatescraper.items import ChocolateProduct
from chocolatescraper.itemloaders import ChocolateProductLoader
from urllib.parse import urlencode

API_KEY = '4e20347b-c58a-4e53-9179-3edb9956baca'


def get_proxy_url(url):
     payload = {'api_key' :API_KEY, 'url': url}
     proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
     return proxy_url
     
class ChocoloateSpider(scrapy.Spider):
    name = "chocoloatespider"
    allowed_domains = ["chocolate.co.uk","proxy.scrapeops.io"]


    def start_requests(self):
      start_url = "https://chocolate.co.uk/collections/all"
      yield scrapy.Request(url = get_proxy_url(start_url), callback= self.parse)     
    
      
    def parse(self, response):
        # Corrected selector for products
        products = response.css('.product-item')

        for product in products:
            chocolate = ChocolateProductLoader(item=ChocolateProduct(), selector=product)
            chocolate.add_css('name', 'a.product-item-meta__title::text')
            chocolate.add_css('price', 'span.price', re=r'<span class="price">\n\s*<span class="visually-hidden">Sale price</span>(.*)</span>')
            chocolate.add_css('url', 'div.product-item-meta a::attr(href)')
            
            # Call load_item method correctly
            yield chocolate.load_item()

        # Follow pagination link
        next_page = response.css('[rel="next"]::attr(href)').get()
        if next_page is not None:
            yield response.follow(get_proxy_url(next_page) , callback=self.parse)
