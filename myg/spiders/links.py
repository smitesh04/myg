import re
from typing import Iterable

import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from myg.db_config import DbConfig
obj = DbConfig()



class LinksSpider(scrapy.Spider):
    name = "links"
    # allowed_domains = ["."]
    # start_urls = ["https://www.myg.in/sitemap.xml"]
    def start_requests(self):
        url = "https://www.myg.in/sitemap.xml"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        links = re.findall('<loc>.*?</loc>', response.text)
        for link in links:
            link_type = ''
            link = link.replace('<loc>', '').replace('</loc>', '')
            if link.count('/') == 4:
                link_type = 'category'
            if link.count('/') == 5:
                link_type = 'subcategory'
            if link.count('/') == 6:
                link_type = 'subsubcategory'
            if link.count('/') == 7:
                link_type = 'product'
            if link_type:
                try:
                    obj.cur.execute(f"insert into {obj.pl_table}(link, type) values('{link}', '{link_type}')")
                    obj.con.commit()
                    print(link_type, link)
                except Exception as e:print(e)





        print()


if __name__ == '__main__':
    ex("scray crawl links".split())
