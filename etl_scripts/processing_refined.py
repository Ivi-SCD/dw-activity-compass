import pandas as pd


TRUSTED_PATH = '../output/pd/trusted/'
SUFIX = '_trusted.csv'
REFINED_PATH = '../output/pd/refined/'

ds_customers = pd.read_csv(TRUSTED_PATH + 'customers' + SUFIX)
ds_products = pd.read_csv(TRUSTED_PATH + 'products' + SUFIX)
ds_items = pd.read_csv(TRUSTED_PATH + 'items' + SUFIX)
ds_payments = pd.read_csv(TRUSTED_PATH + 'payments' + SUFIX)
ds_reviews = pd.read_csv(TRUSTED_PATH + 'reviews' + SUFIX)
ds_orders = pd.read_csv(TRUSTED_PATH + 'orders' + SUFIX)

## Alterações no Dataset Principal

ds_orders.drop(columns=["order_id"], inplace=True)
ds_orders.rename(columns={'order_id_int':'order_id'}, inplace=True)
ds_orders['order_purchase_timestamp'] = pd.to_datetime(ds_orders['order_purchase_timestamp'])
ds_orders['order_approved_at'] = pd.to_datetime(ds_orders['order_approved_at'])
ds_orders['order_delivered_customer_date'] = pd.to_datetime(ds_orders['order_delivered_customer_date'])
ds_orders['order_delivered_carrier_date'] = pd.to_datetime(ds_orders['order_delivered_carrier_date'])
ds_orders['order_estimated_delivery_date'] = pd.to_datetime(ds_orders['order_estimated_delivery_date'])
ds_items['shipping_limit_date'] = pd.to_datetime(ds_items['shipping_limit_date'])

mask = (ds_orders['order_approved_at'].isnull()) & (ds_orders['order_status'] == 'delivered')
ds_orders.loc[mask, 'order_approved_at'] = ds_orders.loc[mask, 'order_purchase_timestamp']

mask = (ds_orders['order_approved_at'].isnull())
ds_orders.loc[mask, 'order_approved_at'] = pd.Timestamp('1900-12-31')
ds_orders.loc[mask, 'order_delivered_carrier_date'] = pd.Timestamp('1900-12-31')
ds_orders.loc[mask, 'order_delivered_customer_date'] = pd.Timestamp('1900-12-31')

mask = (ds_orders['order_delivered_carrier_date'].isnull()) & (ds_orders.order_approved_at != pd.NaT)
ds_orders.loc[mask, 'order_delivered_carrier_date'] = pd.Timestamp('1900-12-31')
ds_orders.loc[mask, 'order_delivered_customer_date'] = pd.Timestamp('1900-12-31')

mask = (ds_orders['order_delivered_customer_date'].isnull())
ds_orders.loc[mask, 'order_delivered_customer_date'] = pd.Timestamp('1900-12-31')

ds_orders = ds_orders.convert_dtypes()

## Dimensões de Tempo

ds_reviews['review_answer_timestamp'] = pd.to_datetime(ds_reviews['review_answer_timestamp'])
ds_reviews['review_creation_date'] = pd.to_datetime(ds_reviews['review_creation_date'])

dim_tempo_instante = pd.DataFrame({
    'tempo_instante': pd.concat([
        ds_orders['order_purchase_timestamp'],
        ds_orders['order_approved_at'],
        ds_orders['order_delivered_carrier_date'],
        ds_orders['order_delivered_customer_date'],
        ds_orders['order_estimated_delivery_date'],
        ds_reviews['review_answer_timestamp'],
        ds_reviews['review_creation_date'],
        ds_items['shipping_limit_date']
    ]).unique()
})

dim_tempo_instante['hora'] = dim_tempo_instante['tempo_instante'].dt.hour
dim_tempo_instante['minuto'] = dim_tempo_instante['tempo_instante'].dt.minute
dim_tempo_instante['segundo'] = dim_tempo_instante['tempo_instante'].dt.second

dim_tempo_data = pd.DataFrame({
    'tempo_data': pd.concat([
        ds_orders['order_purchase_timestamp'].dt.date,
        ds_orders['order_approved_at'].dt.date,
        ds_orders['order_delivered_carrier_date'].dt.date,
        ds_orders['order_delivered_customer_date'].dt.date,
        ds_orders['order_estimated_delivery_date'].dt.date,
        ds_reviews['review_creation_date'].dt.date,
        ds_reviews['review_answer_timestamp'].dt.date,
        ds_items['shipping_limit_date'].dt.date
    ]).unique()
})

dim_tempo_data['dia'] = pd.to_datetime(dim_tempo_data['tempo_data']).dt.day
dim_tempo_data['mes'] = pd.to_datetime(dim_tempo_data['tempo_data']).dt.month
dim_tempo_data['ano'] = pd.to_datetime(dim_tempo_data['tempo_data']).dt.year

dim_tempo_instante.reset_index(inplace=True)
dim_tempo_instante.rename(columns={'index': 'tempo_instante_id'}, inplace=True)
dim_tempo_instante.drop(columns=['tempo_instante'], inplace=True)
dim_tempo_instante = dim_tempo_instante.drop_duplicates(subset=['hora', 'minuto', 'segundo'])

dim_tempo_data.reset_index(inplace=True)
dim_tempo_data.rename(columns={'index': 'tempo_data_id'}, inplace=True)
dim_tempo_data.drop(columns=['tempo_data'], inplace=True)

dim_tempo_instante.to_csv(REFINED_PATH + 'dim_tempo_instante.csv', index=False)
dim_tempo_data.to_csv(REFINED_PATH + 'dim_tempo_data.csv', index=False)

## Dimensão Cliente

dim_cliente = ds_customers.copy()
dim_cliente.drop_duplicates(subset=['customer_id'], keep='first', inplace=True)
dim_cliente.rename(columns={'customer_id': 'cliente_id', 'customer_zip_code_prefix':'cliente_codigo_postal', 
                                                    'customer_city':'cliente_cidade', 'customer_state':'cliente_estado'}, inplace=True)
dim_cliente.to_csv(REFINED_PATH + 'dim_cliente.csv', index=False)

## Dimensão Pagamento

dim_pagamento = ds_payments[['payment_id', 'payment_type']].copy()
dim_pagamento.rename(columns={'payment_id':'pagamento_id', 'payment_type':'pagamento_tipo'},inplace=True)
dim_pagamento.drop_duplicates(subset=['pagamento_tipo'], keep='first', inplace=True)
dim_pagamento['pagamento_id'] = range(1, len(dim_pagamento) + 1)

ds_payments = pd.merge(ds_payments, dim_pagamento, left_on='payment_type', right_on='pagamento_tipo')
ds_payments.drop(columns=['payment_id', 'payment_type', 'pagamento_tipo'], inplace=True)
ds_payments.rename(columns={'payment_sequential':'pagamento_formas_distintas',
                           'payment_installments':'pagamento_parcelamentos',
                           'payment_value':'pagamento_valor'}, inplace=True)

dim_pagamento.to_csv(REFINED_PATH + 'dim_pagamento.csv', index=False)

## Dimensão Pedido

dim_pedido = ds_orders[['order_id', 'order_status']].copy()
dim_pedido.rename(columns={'order_id':'pedido_id', 'order_status':'pedido_status'}, inplace=True)
dim_pedido.drop_duplicates(subset=['pedido_status'], keep='first', inplace=True)
dim_pedido['pedido_id'] = range(1, len(dim_pedido) + 1)

for field in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']:
    ds_orders[field + '_dia'] = ds_orders[field].dt.day
    ds_orders[field + '_mes'] = ds_orders[field].dt.month
    ds_orders[field + '_ano'] = ds_orders[field].dt.year
    ds_orders[field + '_hora'] = ds_orders[field].dt.hour
    ds_orders[field + '_minuto'] = ds_orders[field].dt.minute
    ds_orders[field + '_segundo'] = ds_orders[field].dt.second

def obter_ids(dataframe, campo_base):
    dataframe = pd.merge(ds_orders, dim_tempo_data, how='left',
                         left_on=[campo_base + '_ano', campo_base + '_mes', campo_base + '_dia'],
                         right_on=['ano', 'mes', 'dia'])

    dataframe = dataframe.rename(columns={'tempo_data_id': campo_base + '_data_id'})
    
    dataframe.drop(columns=['ano', 'mes', 'dia'], inplace=True)
    
    dataframe = pd.merge(dataframe, dim_tempo_instante, how='left', 
                         left_on=[campo_base + '_hora', campo_base + '_minuto', campo_base + '_segundo'],
                         right_on=['hora', 'minuto', 'segundo'])
    dataframe = dataframe.rename(columns={'tempo_instante_id': campo_base + '_instante_id'})
    
    dataframe.drop(columns=['hora', 'minuto', 'segundo'], inplace=True)

    return dataframe

for field in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']:
    ds_orders = obter_ids(ds_orders, field)

colunas_aux = []
for field in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']:
    colunas_aux.extend([field + '_ano', field + '_mes', field + '_dia', field + '_hora', field + '_minuto', field + '_segundo'])
colunas_aux.extend(['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date'])

ds_orders.drop(columns=colunas_aux, inplace=True)

ds_orders['order_estimated_delivery_date_dia'] = ds_orders['order_estimated_delivery_date'].dt.day
ds_orders['order_estimated_delivery_date_mes'] = ds_orders['order_estimated_delivery_date'].dt.month
ds_orders['order_estimated_delivery_date_ano'] = ds_orders['order_estimated_delivery_date'].dt.year

ds_orders = pd.merge(ds_orders, dim_tempo_data, how='left',
                         left_on=['order_estimated_delivery_date_ano', 'order_estimated_delivery_date_mes', 'order_estimated_delivery_date_dia'],
                         right_on=['ano', 'mes', 'dia'])

ds_orders.drop(columns=['ano', 'mes', 'dia', 
                        'order_estimated_delivery_date', 
                        'order_estimated_delivery_date_dia', 
                        'order_estimated_delivery_date_mes', 
                        'order_estimated_delivery_date_ano'], inplace=True)

ds_orders.rename(columns={'tempo_data_id': 'pedido_entrega_estimada_data_id'}, inplace=True)

ds_orders = pd.merge(ds_orders, dim_pedido, left_on='order_status', right_on='pedido_status')
ds_orders.rename(columns={
    'order_purchase_timestamp_data_id':'pedido_compra_data_id',
    'order_purchase_timestamp_instante_id':'pedido_compra_instante_id',
    'order_approved_at_data_id':'pedido_aprovacao_data_id',
    'order_approved_at_instante_id':'pedido_aprovacao_instante_id',
    'order_delivered_carrier_date_data_id':'pedido_entregue_operador_data_id',
    'order_delivered_carrier_date_instante_id':'pedido_entregue_operador_instante_id',
    'order_delivered_customer_date_data_id':'pedido_entregue_cliente_data_id',
    'order_delivered_customer_date_instante_id':'pedido_entregue_cliente_instante_id',
    'customer_id':'cliente_id',
}, inplace=True)

ds_orders.drop(columns='order_status', inplace=True)

dim_pedido.to_csv(REFINED_PATH + 'dim_pedido.csv', index=False)

## Dimensão Produto

dim_produto = ds_products.rename(columns={'product_id':'produto_id',
                                          'product_category_name':'produto_categoria', 
                                          'product_name_lenght':'produto_tamanho_nome',
                                          'product_description_lenght':'produto_tamanho_descricao', 
                                          'product_photos_qty':'produto_qtd_fotos', 
                                          'product_weight_g':'produto_peso_g',
                                          'product_length_cm':'produto_largura_cm', 
                                          'product_height_cm':'produto_altura_cm', 
                                          'product_width_cm':'produto_comprimento_cm'
                                          }).copy()

dim_produto.drop(columns=['produto_tamanho_nome', 'produto_tamanho_descricao', 'produto_qtd_fotos', 'produto_peso_g', 'produto_largura_cm', 'produto_altura_cm', 'produto_comprimento_cm'], inplace=True)
dim_produto.to_csv(REFINED_PATH + 'dim_produto.csv', index=False)

## Tabela Fato

ds_items['shipping_limit_date'] = pd.to_datetime(ds_items['shipping_limit_date'])

ds_items['shipping_limit_date_dia'] = ds_items['shipping_limit_date'].dt.day
ds_items['shipping_limit_date_mes'] = ds_items['shipping_limit_date'].dt.month
ds_items['shipping_limit_date_ano'] = ds_items['shipping_limit_date'].dt.year
ds_items['shipping_limit_date_hora'] = ds_items['shipping_limit_date'].dt.hour
ds_items['shipping_limit_date_minuto'] = ds_items['shipping_limit_date'].dt.minute
ds_items['shipping_limit_date_segundo'] = ds_items['shipping_limit_date'].dt.second

# Data

ds_items = pd.merge(ds_items, dim_tempo_data, how='left',
                         left_on=['shipping_limit_date_ano', 'shipping_limit_date_mes', 'shipping_limit_date_dia'],
                         right_on=['ano', 'mes', 'dia'])

ds_items.drop(columns=['ano', 'mes', 'dia', 
                        'shipping_limit_date', 
                        'shipping_limit_date_dia', 
                        'shipping_limit_date_mes', 
                        'shipping_limit_date_ano'], inplace=True)

# Instante

ds_items = pd.merge(ds_items, dim_tempo_instante, how='left',
                         left_on=['shipping_limit_date_hora', 'shipping_limit_date_minuto', 'shipping_limit_date_segundo'],
                         right_on=['hora', 'minuto', 'segundo'])

ds_items.drop(columns=['hora', 'minuto', 'segundo', 
                        'shipping_limit_date_hora', 
                        'shipping_limit_date_minuto', 
                        'shipping_limit_date_segundo'], inplace=True)


ds_items.rename(columns={'tempo_data_id': 'envio_limite_data_id', 
                         'tempo_instante_id':'envio_limite_instante_id',
                        'price':'preco',
                        'freight_value':'frete_valor'}, inplace=True)



fato_pedido = pd.merge(ds_orders, ds_customers['customer_id'], right_on='customer_id', left_on='cliente_id')
fato_pedido = pd.merge(fato_pedido, ds_payments, on='order_id')
fato_pedido = pd.merge(fato_pedido, ds_items, on='order_id')
fato_pedido = pd.merge(fato_pedido, ds_reviews, on='order_id')
fato_pedido.drop(columns=[
    'review_score',
    'review_comment_title',
    'review_comment_message',
    'review_creation_date',
    'review_answer_timestamp',
    'review_id',
    'order_id',
    'customer_id',
    'pedido_status',
    'pedido_entregue_operador_data_id',
    'pedido_entregue_operador_instante_id',
    'pedido_entregue_cliente_data_id',
    'pedido_entregue_cliente_instante_id',
    'pedido_entrega_estimada_data_id',
    'envio_limite_data_id',
    'envio_limite_instante_id'], inplace=True)

fato_pedido.rename(columns={'product_id': 'produto_id', 'preco':'preco_unitario', 'qtd':'quantidade'}, inplace=True)
fato_pedido['fato_pedido_id'] = range(1, len(fato_pedido) + 1)
fato_pedido = fato_pedido.convert_dtypes()

fato_pedido.to_csv(REFINED_PATH + 'fato_pedido.csv', index=False)