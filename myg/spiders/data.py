import datetime
import json
import re
import sys
from typing import Iterable

import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from myg.items import MygItem
import os
from myg.db_config import DbConfig
from myg.common_func import headers
from myg.common_func import create_md5_hash, page_write
obj = DbConfig()
today_date = datetime.datetime.today().strftime('%d_%m_%Y')


class DataSpider(scrapy.Spider):
    name = "data"
    # allowed_domains = ["."]
    # start_urls = ["https://."]

    def __init__(self, start, end):
        self.start = start
        self.end = end


    def start_requests(self):
        # obj.cur.execute(f"select * from {obj.pl_table} where status=0 limit {self.start},{self.end}")
        obj.cur.execute(f"select * from {obj.pl_table} where link='https://www.myg.in/home-and-kitchen/mixer-juicer-grinder/panasonic-ae-475-750-watts-monster-mixer-grinder-red/' limit {self.start},{self.end}")
        rows = obj.cur.fetchall()
        for row in rows:
            link = row['link']
            hashid = create_md5_hash(link)
            pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/{obj.database}/{today_date}"
            file_name = fr"{pagesave_dir}/{hashid}.html"
            row['hashid'] = hashid
            row['pagesave_dir'] = pagesave_dir
            row['file_name'] = file_name
            if os.path.exists(file_name):
                yield scrapy.Request(url='file:///' + file_name, callback=self.parse, cb_kwargs=row, dont_filter=True)
            else:
                yield scrapy.Request(link, headers=headers(), callback=self.parse, cb_kwargs=row)

    def parse(self, response, **kwargs):
        filename = kwargs['file_name']
        pagesave_dir = kwargs['pagesave_dir']

        if not os.path.exists(filename):
            page_write(pagesave_dir, filename, response.text)

        sku = response.xpath('//meta[@itemprop="sku"]/@content').get()
        product_name = response.xpath('//meta[@itemprop="name"]/@content').get()
        product_images_list = response.xpath("//meta[@itemprop='image']/@content").getall()
        product_images = ' | '.join(product_images_list)
        product_id_raw = response.xpath("//div[contains(@class,'ty-account-info__orders')]//input[@name='return_url']/@value").get()
        if not product_id_raw:
            product_id_raw = response.xpath("//input[@name='return_url']/@value").get()
        product_id = re.findall('product_id=.*?&', product_id_raw)[0]
        product_id = product_id.replace('product_id=', '').replace('&', '')

        # catalogue_name = kwargs['link'].split('/')[-2]
        catalogue_name = product_name
        catalogue_id = ''
        # product_name = response.xpath()
        category_hierachy_list = response.xpath('//div[@id="breadcrumbs_11"]//span[@itemprop="itemListElement"]//bdi/text()').getall()
        category_hierachy_dict = dict()
        for lvl, cat in zip(range(1,len(category_hierachy_list)+1), category_hierachy_list):
            category_hierachy_dict[f'l{lvl}'] = cat

        category_hierachy_ = category_hierachy_list[1:]
        category = category_hierachy_list[1]
        category_hierachy = ' | '.join(category_hierachy_)
        discounted_price_list = response.xpath('//div[@class="ty-product-prices"]//span[@class="ty-price-num"]/text()').getall()
        discounted_price = ''.join(discounted_price_list)
        discounted_price = discounted_price.replace(',', '').replace('₹', '').strip()

        stock_info = response.xpath("//div[contains(@class,'ty-product-detail')]//span[contains(@class,'ty-qty-in-stock')]/text()[1]").get()
        if stock_info:
            if 'in stock' in stock_info.lower():
                stock = 'FALSE'
            else:
                stock = 'TRUE'
        else:
            stock = 'TRUE'
        discount_amount = response.xpath("//div[contains(@class,'ty-product-detail')]//span[contains(@class,'ty-save-price')]/bdi/span/text()").get()
        discount_percentage = response.xpath("//div[contains(@class,'ty-product-detail')]//span[contains(@class,'ty-save-price')]/bdi/../span/text()").get()
        if not discount_percentage:
            discount_percentage = ''
        mrp_amount_list = response.xpath("//div[contains(@class,'ty-product-detail')]//span[contains(@class,'ty-strike')]/bdi//span/text()").getall()
        mrp_amount = ''.join(mrp_amount_list)
        mrp_amount = mrp_amount.replace(',', '').replace('₹', '').strip()
        product_url = kwargs['link']
        empty_rating_list = response.xpath("//div[@class='ty-product-block__rating']//i[contains(@class,'ty-icon-star-empty')]").getall()
        try:
            ratings = 5 - len(empty_rating_list)
        except:
            ratings = 0
        try:
            ratings_count = response.xpath("//div[@class='ty-product-block__rating']//a[contains(@class,'ty-discussion__review-a')]/text()").get()
            ratings_count = ratings_count.replace('reviews', '')
            ratings_count = ratings_count.strip()
        except:
            ratings_count = 0

        # manufacturing_info_path = response.xpath("//div[@class='ty-product-feature-group']/h3[contains(text(), 'Manufacturing')]/..//div[@class='ty-product-feature']/div[contains(@class,'ty-product-feature__label')]")
        # manufacturing_info_jsn = {}
        # for manufacture in manufacturing_info_path:
        #     manufacture_label = manufacture.xpath("./div[contains(@class,'ty-product-feature__label')]/text()").get()
        #     manufacture_value = manufacture.xpath("./div[contains(@class,'ty-product-feature__value')]/text()").get()
        #     manufacturing_info_jsn[manufacture_label.strip()] = manufacture_value.strip()

        # general_info_path = response.xpath("//div[@class='ty-product-feature-group']/h3[contains(text(), 'General')]/..//div[@class='ty-product-feature']/div[contains(@class,'ty-product-feature__label')]")
        # general_info_jsn = {}
        # for general in general_info_path:
        #     general_label = general.xpath("./div[contains(@class,'ty-product-feature__label')]/text()").get()
        #     general_value = general.xpath("./div[contains(@class,'ty-product-feature__value')]/text()").get()
        #     general_info_jsn[general_label.strip()] = general_value.strip()

        brand = response.xpath("//div[@class='ty-product-feature__label' and contains(text(),'Brand:')]//following-sibling::div/text()").get()
        feature_path = response.xpath("//div[@id='content_features']//div[@class='ty-product-feature-group']")
        features_list = list()

        features_dict = {}
        for feature in feature_path:
            feature_header = feature.xpath("./h3[@class='ty-subheader']/text()").get()
            feature_header = feature_header.strip()
            feature_dict_sub = {}
            feature_path_sub = feature.xpath("./div[@class='ty-product-feature']")
            for feature_ in feature_path_sub:
                feature_label = feature_.xpath('.//div[@class="ty-product-feature__label"]/text()').get()
                feature_label = feature_label.replace(':', '').strip()
                feature_value = feature_.xpath('.//div[@class="ty-product-feature__value"]//text()').get()
                feature_value = feature_value.strip()
                feature_dict_sub[feature_label] = feature_value
            features_dict[feature_header] = feature_dict_sub
        features_dict['brand'] = brand

        description1_raw = response.xpath("//div[@id='content_description']//div[contains(@class,'feature_modular_map_desc')]//text()").getall()
        description1_list = list()
        for desc in description1_raw:
            desc = desc.strip()
            if desc and desc not in description1_list:
                description1_list.append(desc)
        description1 = ' | '.join(description1_list)

        description2_raw = response.xpath("//div[@id='content_description']//p//text()").getall()
        description2_list = list()
        for desc in description2_raw:
            desc = desc.strip()
            if desc and desc not in description2_list:
                description2_list.append(desc)
        description2 = ' | '.join(description2_list)

        description = ''
        if description1:
            descripiton = description1
        elif description2:
            description = description2

        others_jsn = {}
        others_jsn['MOQ'] = '1'
        others_jsn['delivery'] = 'myg_logout'
        others_jsn['data_vendor'] = 'Actowiz'
        others_jsn['brand'] = brand
        others_jsn['images'] = product_images_list
        # others_jsn['manufacturing_info'] = manufacturing_info_jsn
        # others_jsn['general'] = general_info_jsn
        if description:
            others_jsn['Description'] = description
        others_jsn['product_id'] = product_id
        others_jsn['maximum_retail_price'] = mrp_amount
        others_jsn['selling_price'] = discounted_price
        others_jsn['product_info'] = features_dict

        others_jsn = json.dumps(others_jsn)




        item = MygItem()
        item['input_pid'] = 'N/A'
        item['product_id'] = product_id
        item['catalog_name'] = catalogue_name
        item['catalog_id'] = product_id
        item['product_name'] = product_name
        item['image_url'] = product_images_list[0]
        item['category_hierarchy'] = json.dumps(category_hierachy_dict)
        if not discounted_price:
            discounted_price = 'N/A'
        item['product_price'] = discounted_price
        item['shipping_charges'] = 'N/A'
        item['is_sold_out'] = stock
        if not discount_percentage:
            discount_percentage = 'N/A'
        item['discount'] = discount_percentage
        if not mrp_amount:
            mrp_amount = 'N/A'
        item['mrp'] = mrp_amount
        item['page_url'] = 'N/A'
        item['product_url'] = product_url
        if not ratings_count or ratings_count==0 or ratings_count=='0':
            ratings_count = 'N/A'
        item['number_of_ratings'] = ratings_count
        if not ratings or ratings==0 or ratings=='0':
            ratings = 'N/A'
        item['avg_rating'] = ratings
        item['position'] = 'N/A'
        item['country_code'] = 'IN'
        item['others'] = others_jsn
        item['category'] = category


        yield item

if __name__ == '__main__':
    try:
        start = sys.argv[1]
        end = sys.argv[2]
    except:
        start = 0
        end = 10
    ex(f'scrapy crawl data -a start={start} -a end={end}'.split())
