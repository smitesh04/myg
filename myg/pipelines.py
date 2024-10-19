# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from myg.items import MygItem
from myg.db_config import DbConfig
obj = DbConfig()

class MygPipeline:
    def process_item(self, item, spider):
        if isinstance(item, MygItem):
            obj.insert_data(item)
        return item
