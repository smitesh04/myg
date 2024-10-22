import sys

import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from myg.db_config import DbConfig
from myg.common_func import headers

obj = DbConfig()
import scrapy


class LinksProductSpider(scrapy.Spider):
    name = "links_product"
    handle_httpstatus_list = [404]
    # handle_httpstatus_list = [500]

    # allowed_domains = ["."]
    # start_urls = ["https://."]
    def __init__(self, start, end):
        self.page = 1
        self.start = start
        self.end = end



    def start_requests(self):
        obj.cur.execute(f"select * from {obj.pl_table_sitemap} where status=0 and type='category' limit {self.start},{self.end}")

        rows = obj.cur.fetchall()
        for row in rows:
            url = row['link']
            row['url'] = url
            # url = 'https://www.myg.in/personal-care/page1/?activate_redirect=0'
            yield scrapy.Request(url, headers=headers(), callback=self.parse, cb_kwargs=row)

    def parse(self, response, **kwargs):

        products_check = response.xpath("//div[@id='category_products_11']")
        if products_check:
            all_products = response.xpath("//div[@class='col-tile']")
            for product in all_products:
                link = product.xpath('.//a[@class="product-title"]/@href').get()
                if link:
                    try:
                        obj.cur.execute(f"insert into {obj.pl_table}(link) values('{link}')")
                        obj.con.commit()
                        print(link)
                    except Exception as e:print(e)
            if self.page < 500:
                self.page +=1
                print(self.page)
                url = kwargs['url']+f'page-{self.page}/?activate_redirect=0'
                yield scrapy.Request(url, headers=headers(), callback=self.parse, cb_kwargs=kwargs)
        else:
            obj.cur.execute(f"update {obj.pl_table_sitemap} set status='1' where link='{kwargs['link']}'")
            obj.con.commit()


if __name__ == '__main__':
    try:
        start = sys.argv[1]
        end = sys.argv[2]
    except:
        start = 0
        end = 1
    ex(f'scrapy crawl links_product -a start={start} -a end={end}'.split())