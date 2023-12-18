import time
import logging
import scrapy
import re
import datetime as dt
import scrapydo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from scrapy.item import Item, Field
from scrapy.selector import Selector
from scrapy.http.request import Request
from bs4 import BeautifulSoup
from datetime import timedelta
from google.cloud.bigquery.table import TimePartitioning
from google.cloud.bigquery.table import TimePartitioningType

bigquery_client = bigquery.Client(project='project_id')

#initiate constant
project_id = 'project_id'
dataset_id = 'news'
table_id = 'news_raw'

dataset_ref = '{}.{}'.format(project_id, dataset_id)
table_ref = '{}.{}.{}'.format(project_id, dataset_id, table_id)
results = []

def run_single_crawl(request):
    scrapydo.setup()
    initiate_bigquery()
    results = scrapydo.run_spider(KompasSpider)
    pass

def create_dataset_if_not_exist(client, dataset_id):
   try:
      dataset = client.get_dataset(dataset_id)  # Make an API request.
      print("Dataset {} already exists".format(dataset_id))
   except NotFound:
      print("Dataset {} not found".format(dataset_id))
      dataset = bigquery.Dataset(dataset_id)
      dataset.location = "US"
      dataset = client.create_dataset(dataset)
      print("Dataset {} is has created".format(dataset_id))

   return dataset

def create_table_if_not_exist(client, table_id):
   try:
      client.get_table(table_id)  # Make an API request.
      print("Table {} already exists.".format(table_id))
   except NotFound:
      print("Table {} not found".format(dataset_id))

      schema = [
         bigquery.SchemaField("source_name", "STRING"),
         bigquery.SchemaField("title", "STRING"),
         bigquery.SchemaField("description", "STRING"),
         bigquery.SchemaField("link", "STRING"),
         bigquery.SchemaField("image_url", "STRING"),
         bigquery.SchemaField("category", "STRING"),
         bigquery.SchemaField("published_at", "TIMESTAMP"),
         bigquery.SchemaField("content", "STRING"),
         bigquery.SchemaField("author", "STRING"),
         bigquery.SchemaField("editor", "STRING"),
         bigquery.SchemaField("keywords", "STRING"),
      ]

      bq_table = bigquery.Table(table_id, schema=schema)
      print("initiate Table {} with schema {}".format(dataset_id, schema))

      bq_table.time_partitioning = TimePartitioning(
         type_=TimePartitioningType.DAY, field='published_at'
      )
      print("update Table {} to use time partition".format(dataset_id))

      client.create_table(bq_table)
      print("Table {} has created".format(table_id))

   pass

def initiate_bigquery():
   print("start initiate_bigquery_function")
   dataset_ref = '{}.{}'.format(project_id, dataset_id)
   table_ref = '{}.{}.{}'.format(project_id, dataset_id, table_id)

   #check dataset exists
   create_dataset_if_not_exist(bigquery_client, dataset_ref)

   #check table existst
   create_table_if_not_exist(bigquery_client, table_ref)

   print("finish initiate_bigquery_function")
   pass

class JsonWriterPipeline(object):
   def close_spider(self, spider):
      start_time = time.time()
      print("start uploaded {} rows to bigquery".format(len(results)))
      bigquery_client.insert_rows_json(table_ref, results)
      print("took {} uploaded {} rows to bigquery".format(time.time() - start_time, len(results)))

   def process_item(self, item, spider):
      results.append(dict(item))
      pass

class KompasItem(Item):
   source_name = Field()
   title = Field()
   description = Field()
   link = Field()
   image_url = Field()
   category = Field()
   published_at = Field()
   content = Field()
   keywords = Field()
   author = Field()
   editor = Field()

def convert_time_format_to_utc(time):
   time_arr = re.split('[\s,]', time)
   filter_object = filter(lambda x: x != "", time_arr)
   time_arr = list(filter_object)[:2]
   info_time = ' '.join([s for s in time_arr if s])

   published_at_wib = dt.datetime.strptime(info_time, '%d/%m/%Y %H:%M')
   result = published_at_wib + timedelta(hours=-7)
   return result.isoformat()

class KompasSpider(scrapy.Spider):
   name = "kompas"
   allowed_domains = ["kompas.com"]

   # this utc time should be yesterday on indonesia time
   yesterday_date_string = (dt.datetime.today()).strftime('%Y-%m-%d')
   start_urls = [
      "https://indeks.kompas.com/?site=all&date={}".format(yesterday_date_string)
   ]
   custom_settings = {
      'LOG_LEVEL': logging.WARNING,
      'ITEM_PIPELINES': {'main.JsonWriterPipeline': 1}
   }

   def parse(self, response):
      start_time = time.time()
      indeks = Selector(response).xpath('//div[@class="article__list clearfix"]')
      total_news_onpage = len(indeks)

      for indek in indeks:
         item = KompasItem()
         news_link = indek.xpath('div[@class="article__list__title"]/h3/a/@href').extract_first() + '?page=all'
         item['source_name'] = "kompas.com"
         item['title'] = indek.xpath('div[@class="article__list__title"]/h3/a/text()').extract_first()
         item['link'] = news_link
         item['image_url'] = indek.xpath('div[@class="article__list__asset clearfix"]/div/a/img/@src').extract_first()
         item['category'] = indek.xpath('div[@class="article__list__info"]/div[@class="article__subtitle article__subtitle--inline"]/text()').extract_first()
         item['published_at'] = convert_time_format_to_utc(indek.xpath('div[@class="article__list__info"]/div[@class="article__date"]/text()').extract_first())
         detail_request = Request(news_link, callback=self.parse_detail)
         detail_request.meta['item'] = item
         yield detail_request

      next_page = Selector(response).xpath('//a[@class="paging__link paging__link--next"]/@href')[-1].extract()
      is_next = Selector(response).xpath('//a[@class="paging__link paging__link--next"]/text()')[-1].extract() == 'Next'
      if next_page and is_next:
         yield Request(next_page, callback=self.parse)

      print("running {} news on url {} taking {} seconds".format(total_news_onpage, response.url, time.time() - start_time))

   def parse_detail(self, response):
      item = response.meta['item']
      item['description'] = Selector(response).xpath('//head/meta[@name="description"]/@content').extract_first()
      content = Selector(response).xpath('//div[@class="read__content"]').extract_first()
      item['content'] = BeautifulSoup(content).text
      item['author'] = Selector(response).xpath('//head/meta[@name="content_author"]/@content').extract_first()
      item['editor'] = Selector(response).xpath('//head/meta[@name="content_editor"]/@content').extract_first()
      item['keywords'] = Selector(response).xpath('//head/meta[@name="keywords"]/@content').extract_first()
      return item

if __name__ == "__main__":
    print("Running local test...")
    run_single_crawl(None)
    print("Local test complete.")
