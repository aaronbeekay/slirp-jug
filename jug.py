import shopify
import logging
import os
import psycopg2

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
tablequery = '''
CREATE TABLE IF NOT EXISTS variants (
	variant_id BIGINT PRIMARY KEY,
	product_id BIGINT, 
	product_title TEXT, 
	product_body_html TEXT,
	product_handle TEXT,
	product_publish_date TIMESTAMPTZ,
	product_update_date TIMESTAMPTZ,
	product_vendor TEXT,
	product_tags TEXT,
	product_manufacturer TEXT,
	product_mpn TEXT,
	product_dim_x REAL,
	product_dim_y REAL,
	product_dim_z REAL,
	product_shipping_notes TEXT,
	variant_barcode INTEGER,
	variant_creation_date TIMESTAMPTZ,
	variant_option1_value TEXT,
	variant_option2_value TEXT,
	variant_option3_value TEXT,
	variant_condition TEXT,
	variant_condition_notes TEXT,
	variant_sunk_cost MONEY,
	variant_reserve_price MONEY,
	variant_ask_price MONEY,
	variant_ownership TEXT,
	variant_price MONEY,
	variant_title TEXT,
	variant_update_date TIMESTAMPTZ,
	variant_weight REAL
	
)
'''
rowquery = '''
INSERT INTO variants 
					(			variant_id,
								product_id, 
								product_title, 
								product_body_html,
								product_handle,
								product_publish_date,
								product_update_date,
								product_vendor,
								product_tags,
								product_manufacturer,
								product_mpn,
								variant_barcode,
								variant_creation_date,
								variant_option1_value,
								variant_option2_value,
								variant_option3_value,
								variant_condition,
								variant_condition_notes,
								variant_sunk_cost,
								variant_reserve_price,
								variant_ask_price,
								variant_ownership,
								variant_price,
								variant_title,
								variant_update_date,
								variant_weight,
								product_dim_x,
								product_dim_y,
								product_dim_z,
								product_shipping_notes			)
								
				VALUES(			%(variant_id)s,
								%(product_id)s, 
								%(product_title)s, 
								%(product_body_html)s,
								%(product_handle)s,
								%(product_publish_date)s,
								%(product_update_date)s,
								%(product_vendor)s,
								%(product_tags)s,
								%(product_manufacturer)s,
								%(product_mpn)s,
								%(variant_barcode)s,
								%(variant_creation_date)s,
								%(variant_option1_value)s,
								%(variant_option2_value)s,
								%(variant_option3_value)s,
								%(variant_condition)s,
								%(variant_condition_notes)s,
								%(variant_sunk_cost)s,
								%(variant_reserve_price)s,
								%(variant_ask_price)s,
								%(variant_ownership)s,
								%(variant_price)s,
								%(variant_title)s,
								%(variant_update_date)s,
								%(variant_weight)s,
								%(product_dim_x)s,
								%(product_dim_y)s,
								%(product_dim_z)s,
								%(product_shipping_notes)s			)	
'''


DATABASE_URL = os.environ['DATABASE_URL']

#import pdb; pdb.set_trace()
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
cur.execute(tablequery)
conn.commit()

# Get products
products = shopify.Product.find()
for product in products:
	logger.debug('Looking at product {} ({})'.format(product.id, product.handle))
	variants = product.variants
	logger.debug('I see {} variants for {}.'.format(len(variants), product.handle))
	
	# Extract metafields
	pmfg = pmpn = px = py = pz = pshippingnotes = None
	for mf in product.metafields():
		if mf.key == 'Manufacturer':
			pmfg = mf.key
		elif mf.key == 'MPN':
			pmpn = mf.key
	
	for variant in variants:
		# Extract data from metafields
		vcond = vcondnotes = vsunkcost = vreserveprice = vaskprice = vownership = None
		vx = vy = vz = shippingnotes = None

		params = {					'variant_id' 				: variant.id,
									'product_id' 				: product.id,
									'product_title' 			: product.title,
									'product_body_html' 		: product.body_html,
									'product_handle' 			: product.handle,
									'product_publish_date' 		: product.published_at,
									'product_update_date' 		: product.updated_at,
									'product_vendor' 			: product.vendor,
									'product_tags' 				: product.tags,
									'product_manufacturer' 		: pmfg,
									'product_mpn' 				: pmpn,
									'variant_barcode' 			: variant.barcode,
									'variant_creation_date' 	: variant.created_at,
									'variant_option1_value' 	: variant.option1,
									'variant_option2_value' 	: variant.option2,
									'variant_option3_value' 	: variant.option3,
									'variant_condition' 		: vcond,
									'variant_condition_notes' 	: vcondnotes,
									'variant_sunk_cost' 		: vsunkcost,
									'variant_reserve_price' 	: vreserveprice,
									'variant_ask_price' 		: vaskprice,
									'variant_ownership' 		: vownership,
									'variant_price' 			: variant.price,
									'variant_title' 			: variant.title,
									'variant_update_date' 		: variant.updated_at,
									'variant_weight' 			: variant.weight,
									'variant_inventory_qty'		: variant.inventory_quantity,
									'product_dim_x' 			: px,
									'product_dim_y' 			: py,
									'product_dim_z' 			: pz,
									'product_shipping_notes' 	: pshippingnotes			
																				}
		cur.execute(rowquery, params)
		conn.commit()
	logger.info('Added {} variants for product {}: {}'.format(	len(variants),
																product.id,
																product.handle) )
