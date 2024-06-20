import pandas as pd

pd.set_option('display.max_columns', None)

PATH_RAW = '../input/raw/'
PATH_TRUSTED = '../output/pd/trusted/'

ds_orders = pd.read_csv(PATH_RAW + 'olist_orders_dataset.csv')
ds_customers = pd.read_csv(PATH_RAW + 'olist_customers_dataset.csv')
ds_payments = pd.read_csv(PATH_RAW + 'olist_order_payments_dataset.csv')
ds_reviews = pd.read_csv(PATH_RAW + 'olist_order_reviews_dataset.csv')
ds_products = pd.read_csv(PATH_RAW + 'olist_products_dataset.csv')

### Customers

ds_customeres_without_id = ds_customers.drop_duplicates(subset=['customer_unique_id'], keep='first')
ds_customeres_without_id = ds_customeres_without_id.drop(columns="customer_id", axis=1)

ds_customeres_without_id['client_id'] = range(1, len(ds_customeres_without_id) + 1)
client_id_map = ds_customeres_without_id.set_index('customer_unique_id')['client_id']
ds_customers['customer_unique_id'] = ds_customers['customer_unique_id'].map(client_id_map)

customer_id_map = ds_customers.set_index('customer_id')['customer_unique_id']
ds_orders['customer_id'] = ds_orders['customer_id'].map(customer_id_map)
ds_customers.drop(columns=['customer_id'], inplace=True)
ds_customers.rename(columns={'customer_unique_id':'customer_id'}, inplace=True)
ds_customers.drop_duplicates(subset=['customer_id'], keep='first')
ds_customers = ds_customers.convert_dtypes()
ds_orders = ds_orders.convert_dtypes()

ds_customers.to_csv(PATH_TRUSTED + 'customers_trusted.csv', index=False)

### Payments

ds_payments['payment_id'] = range(1, len(ds_payments) + 1)
ds_orders['order_id_int'] = range(1, len(ds_orders) + 1)
ds_order_id_map = ds_orders.set_index('order_id')['order_id_int']
ds_payments['order_id'] = ds_payments['order_id'].map(ds_order_id_map)

ds_payments = ds_payments.convert_dtypes()
ds_orders = ds_orders.convert_dtypes()

ds_payments.to_csv(PATH_TRUSTED + 'payments_trusted.csv', index=False)

### 

ds_reviews.fillna({'review_comment_title':'Sem Titulo', 'review_comment_message':'Sem Comentários'}, inplace=True)
ds_reviews.drop_duplicates(subset=['order_id'], inplace=True)

ds_reviews.drop(columns=['review_id'], inplace=True)
ds_reviews['review_id'] = range(1, len(ds_reviews) + 1)

order_id_map = ds_orders.set_index('order_id')['order_id_int']
ds_reviews['order_id'] = ds_reviews['order_id'].map(order_id_map)

ds_reviews = ds_reviews.convert_dtypes()
ds_orders = ds_orders.convert_dtypes()

ds_reviews.to_csv(PATH_TRUSTED + 'reviews_trusted.csv', index=False)
ds_orders.to_csv(PATH_TRUSTED + 'orders_trusted.csv', index=False)

### Products

ds_itens = pd.read_csv(PATH_RAW + 'olist_order_items_dataset.csv')

ds_products.fillna({'product_category_name': 'outros', 'product_name_lenght': 0,
                    'product_description_lenght': 0, 'product_photos_qty': 0}, 
                    inplace=True)

ds_products = ds_products.dropna(subset=['product_weight_g', 
                                         'product_length_cm', 
                                         'product_height_cm', 
                                         'product_width_cm'])

ds_products['product_id_int'] = range(1, len(ds_products) + 1)
product_id_map = ds_products.set_index('product_id')['product_id_int']
ds_itens['product_id'] = ds_itens['product_id'].map(product_id_map)

ds_products.drop(columns=['product_id'], inplace=True)
ds_products.rename(columns={'product_id_int':'product_id'}, inplace=True)

ds_products = ds_products.convert_dtypes()
ds_itens = ds_itens.convert_dtypes()

ds_products.to_csv(PATH_TRUSTED + 'products_trusted.csv', index=False)
ds_itens.to_csv(PATH_TRUSTED + 'itens_trusted.csv', index=False)