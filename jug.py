import shopify
import logging
import os

logger = logging.getLogger('jug.slirp.aaronbeekay')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('jug.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

#import pdb; pdb.set_trace()

try:
	SHOPIFY_API_KEY = os.environ['SHOPIFY_API_KEY']
	SHOPIFY_API_PASSWORD = os.environ['SHOPIFY_API_PASSWORD']
except KeyError:
	raise ValueError('Didn\'t find API key or password in env vars')

shop_url = "https://%s:%s@glitchlab.myshopify.com/admin" % (SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
shopify.ShopifyResource.set_site(shop_url)

# Set up DB table
import os
import psycopg2

DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
tablequery = '''
CREATE TABLE IF NOT EXIST()
'''

# Get products
products = shopify.Product.find()
for product in products:
	logger.debug('Looking at product {} ({})'.format(product.id, product.handle))
	variants = product.variants
	logger.debug('I see {} variants for {}.'.format(len(variants), product.handle))
	for variant in variants:
		query = '''
INSERT INTO {tablename} (	'variant_id',
							'product_id', 
							'product_title', 
							'product_body_html',
							'product_handle',
							'product_publish_date',
							'product_update_date',
							'product_vendor',
							'product_tags',
							'variant_barcode',
							'variant_creation_date',
							'variant_option1_value',
							'variant_option2_value',
							'variant_option3_value',
							'variant_price',
							'variant_title',
							'variant_update_date',
							'variant_weight'			),
				VALUES(		'{variant_id}',
							'{product_id}', 
							'{product_title}', 
							'{product_body_html}',
							'{product_handle}',
							'{product_publish_date}',
							'{product_update_date}',
							'{product_vendor}',
							'{product_tags}',
							'{variant_barcode}',
							'{variant_creation_date}',
							'{variant_option1_value}',
							'{variant_option2_value}',
							'{variant_option3_value}',
							'{variant_price}',
							'{variant_title}',
							'{variant_update_date}',
							'{variant_weight}'			)	
'''
		query = query.format(	tablename = 'butts',
								variant_id = variant.id,
								product_id = product.id,
								product_title = product.title,
								product_body_html = product.body_html,
								product_handle = product.handle,
								product_publish_date = product.published_at,
								product_update_date = product.updated_at,
								product_vendor = product.vendor,
								product_tags = product.tags,
								variant_barcode = variant.barcode,
								variant_creation_date = variant.created_at,
								variant_option1_value = variant.option1,
								variant_option2_value = variant.option2,
								variant_option3_value = variant.option3,
								variant_price = variant.price,
								variant_title = variant.title,
								variant_update_date = variant.updated_at,
								variant_weight = variant.weight			
																			)
		print(query)
