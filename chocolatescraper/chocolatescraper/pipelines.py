# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from psycopg2 import sql
from scrapy.exceptions import DropItem

import psycopg2


class ChocolatescraperPipeline:
    def process_item(self, item, spider):
        return item


class PriceToUSDPipeline:
    gbdToUSDRate = 1.3
    
    def process_item(self, item, spider):
           adapter= ItemAdapter(item)
           
           if adapter.get('price') :
               
                floatPrice = float(adapter['price'])   
     
                adapter['price'] = floatPrice * self.gbdToUSDRate
                
                return item
            
           else:
                raise DropItem(f'Missing price in  {item}') 
                               
                               
                               
class DuplicatesPipeline:                               
    def __init__(self):
        self.names_seen = set()
       
       
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)  
        
        if adapter['name'] in self.names_seen:
            raise DropItem(f'Duplicate Item found:{item!r}')
        
        else :
            self.names_seen.add(adapter['name']) 
            return  item   
     
     

class SavingToPostgresPipeline(object):
    
    def __init__(self):
        self.db_name = "chocolate_scraping"
        self.user = "postgres"
        self.password = "123321"
        self.host = "localhost"
        self.port = "5432"
        self.create_connection()

    def create_connection(self):
        # Establish a connection to PostgreSQL without specifying the database
        conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database="postgres"  # Connect to the default postgres database
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.db_name}';")
        exists = cursor.fetchone()
        
        if not exists:
            # Create the database if it doesn't exist
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(self.db_name)
            ))
            print(f"Database '{self.db_name}' created.")
        else:
            print(f"Database '{self.db_name}' already exists.")

        # Close the connection to the default database
        cursor.close()
        conn.close()

        # Now connect to the created database
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.db_name,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.curr = self.conn.cursor()

    def process_item(self, item, spider):
        self.store_in_db(item)
        return item

    def store_in_db(self, item):
        # Ensure the table exists before inserting items
        self.curr.execute("""
            CREATE TABLE IF NOT EXISTS chocolate_products (
                title TEXT,
                price FLOAT,
                url TEXT
            )
        """)

        # Insert the item data into the database
        self.curr.execute(""" 
            INSERT INTO chocolate_products (title, price, url) VALUES (%s, %s, %s)
        """, (
            item.get("name", [None]),
            item.get("price", [None]),
            item.get("url", [None])
        ))
        self.conn.commit()

    def close_spider(self, spider):
        # Close the database connection when the spider is done
        self.curr.close()
        self.conn.close()
