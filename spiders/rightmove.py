import scrapy
import os
import re
import logging
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from RentSpider import RentSpider

logging.getLogger("scrapy-playwright").setLevel(logging.INFO)

os.chdir(os.path.dirname(os.path.abspath(__file__)))


class RightmoveSpider(RentSpider):
    name = "rightmove"
    ids = []

    def start_requests(self):
        property_rows = [row for row in self.cur.execute("SELECT location_id, postcode FROM rightmove_postcode_map").fetchall()]
        for idx, (location_id, postcode) in enumerate(property_rows):
            yield self._gen_request(postcode, location_id)

    def parse(self, response):
        property_cards = response.css(".propertyCard-wrapper")
        for card in property_cards:
            property_id = card.xpath("../a[@class='propertyCard-anchor']/@id").get()
            if property_id in self.ids:
                # Sometimes playwright iterates over the same card twice, not sure why
                continue
            self.ids.append(property_id)

            address = card.xpath(".//address/meta[@itemprop='streetAddress']/@content").get().replace("'", "''")
            price = re.sub("[^0-9.]", "", card.xpath(".//*[@class='propertyCard-priceValue']/text()").get())
            property_info = card.xpath(".//*[@class='property-information']")
            spans = property_info.xpath("./span")
            property_type = spans[0].xpath("text()").get()
            url = card.xpath(".//*[@class='propertyCard-link']/@href").get()
            full_url = f"https://www.rightmove.co.uk{url}"
            now = datetime.now()

            if property_type == "Flat Share" or "OpenRent" in card.get():
                # Not interested in rooms in a shared flat;
                # We crawl OpenRent separately
                continue

            try:
                num_bedrooms = "1" if property_type == "Studio" else spans[2].xpath("text()").get()
            except:
                # Not lived accommodation
                continue

            try:
                num_bathrooms = spans[2].xpath("text()").get() if property_type == "Studio" else spans[4].xpath("text()").get()
            except:
                # Sometimes Rightmove doesn't list the bathroom number if there's only 1
                num_bathrooms = 1

            self.cur.execute(f"""
                INSERT INTO timeline VALUES
                    ('{address}', '{response.meta["postcode"]}', {price}, {num_bedrooms}, {num_bathrooms}, '{property_type}', '{now.isoformat()}', {now.timestamp()}, '{full_url}', '{property_id}')
            """)

            yield dict(
                address=address,
                price_pcm=price,
                num_bedrooms=num_bedrooms,
                num_bathrooms=num_bathrooms,
                property_type=property_type,
                id=property_id,
                url=full_url,
            )

        next_button = response.xpath(".//*[@data-test='pagination-next']").get()
        if "disabled" not in next_button:
            page_index = parse_qs(urlparse(response.url).query)["index"][0]
            yield self._gen_request(response.meta["postcode"], response.meta["location_id"], int(page_index) + 24)

    def _gen_request(self, postcode, location_id, index=0):
        url = f"https://www.rightmove.co.uk/property-to-rent/find.html?searchType=RENT&locationIdentifier=OUTCODE%5E{location_id}&insId=1&radius=0.0&index={index}"
        return scrapy.Request(
            url=url,
            callback=self.parse,
            meta=dict(postcode=postcode, location_id=location_id, playwright=True)
        )






