import datetime

import pymysql

dd_mm_YYYY = datetime.datetime.today().strftime('%d_%m_%Y')


class DbConfig():

    def __init__(self):
        self.database = 'myg'
        self.con = pymysql.Connect(host='localhost',
                              user='root',
                              password='actowiz',
                              database= self.database)
        self.cur = self.con.cursor(pymysql.cursors.DictCursor)
        self.data_table = f'{self.database}_product_data'
        self.pl_table = f'{self.database}_pl'
        self.pl_table_sitemap = f'{self.database}_pl_sitemap'

    def check_table_exists(self, table_name):
        query = f"SHOW TABLES LIKE '{table_name}';"
        self.cur.execute(query)
        return self.cur.fetchone() is not None

    def create_data_table(self, data_table):
        if not self.check_table_exists(data_table):
            query = f'''
                   CREATE TABLE IF NOT EXISTS `{self.data_table}` (
                  `Id` int NOT NULL AUTO_INCREMENT,
                  `input_pid` varchar(40) DEFAULT 'N/A',
                  `product_id` varchar(40) NOT NULL,
                  `catalog_name` varchar(500) NOT NULL,
                  `catalog_id` varchar(40) NOT NULL,
                  `source` varchar(40) DEFAULT 'amazon',
                  `scraped_date` datetime DEFAULT CURRENT_TIMESTAMP,
                  `product_name` varchar(500) DEFAULT 'N/A',
                  `image_url` varchar(500) DEFAULT 'N/A',
                  `category_hierarchy` json DEFAULT NULL,
                  `product_price` decimal(9,2) DEFAULT NULL,
                  `arrival_date` varchar(40) DEFAULT 'N/A',
                  `shipping_charges` float DEFAULT NULL,
                  `is_sold_out` varchar(40) DEFAULT 'false',
                  `discount` varchar(40) DEFAULT 'N/A',
                  `mrp` decimal(9,2) DEFAULT NULL,
                  `page_url` varchar(5) DEFAULT 'N/A',
                  `product_url` varchar(500) NOT NULL,
                  `number_of_ratings` int DEFAULT NULL,
                  `avg_rating` float DEFAULT NULL,
                  `position` varchar(5) DEFAULT 'N/A',
                  `country_code` varchar(2) DEFAULT 'IN',
                  `others` json DEFAULT NULL,
                  `is_login` int DEFAULT '0',
                  `is_zip` int DEFAULT '0',
                  `zip_code` int DEFAULT '0',
                  `shipping_charges_json` json DEFAULT (json_object()),
                  `product_price_json` json DEFAULT (json_object()),
                  `mrp_json` json DEFAULT (json_object()),
                  `discount_json` json DEFAULT (json_object()),
                  PRIMARY KEY (`Id`),
                  UNIQUE KEY `product_id` (`product_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                '''

            self.cur.execute(query)
            self.con.commit()
            print(f'Table {data_table} has been created! ')


    def insert_data(self, item):
        field_list = []
        value_list = []
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        fields = ','.join(field_list)
        values = ", ".join(value_list)
        insert_db = f"insert into {self.data_table}( " + fields + " ) values ( " + values + " )"
        qr = (insert_db, tuple(item.values()))
        try:
            self.cur.execute(insert_db, tuple(item.values()))
            self.con.commit()

            self.update_pl_status(item['product_url'])
        except Exception as e:
            print(e)

    def update_pl_status(self, link):
        try:
            self.cur.execute(f"update {self.pl_table} set status='1' where link='{link}'")
            self.con.commit()
        except Exception as e:print(e)

DbConfig().create_data_table(DbConfig().data_table)

