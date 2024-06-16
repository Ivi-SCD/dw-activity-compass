import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASS")

df_cliente = pd.read_csv('./output/dim_cliente.csv')
df_pedido_item = pd.read_csv('./output/dim_pedido_item.csv')
df_pedidos_avaliacao = pd.read_csv('./output/dim_pedidos_avaliacao.csv')
df_pedidos_pagamento = pd.read_csv('./output/dim_pedidos_pagamentos.csv')
df_pedidos = pd.read_csv('./output/dim_pedidos.csv')
df_produto = pd.read_csv('./output/dim_produto.csv')
df_fato_pedidos = pd.read_csv('./output/fato_pedidos.csv')


def criar_tabela(nome_tabela, colunas):
    query = f"CREATE TABLE {nome_tabela} ("

    colunas_definicoes = []

    for coluna, tipo in colunas.items():
        colunas_definicoes.append(f"{coluna} {tipo}")

    query += ", ".join(colunas_definicoes)
    query += ");"

    return query


def gerar_query_insert(df, nome_tabela):
    colunas = ", ".join(df.columns)
    query = f"INSERT INTO {nome_tabela} ({colunas}) VALUES\n"
    valores = []
    for _, row in df.iterrows():
        valor = ", ".join([f"'{str(v).replace('\'', '').replace('\"', '')}'" if isinstance(v, str) else str(v) for v in row])
        valores.append(f"({valor})")

    query += ",\n".join(valores) + ";"
    
    return query

if __name__ == '__main__':
    conn = psycopg2.connect(f"dbname={DB} user={USER} password={PASSWORD}")
    print("Conexão realizada com sucesso.")

    campos_cliente = {
        'id_cliente': 'VARCHAR(200) PRIMARY KEY',
        'id_unico_cliente': 'VARCHAR(200)',
        'cod_postal_prefixo': 'VARCHAR(50)',
        'cidade_cliente': 'VARCHAR(100)',
        'estado_cliente': 'VARCHAR(50)'
    }

    campos_pedido_item = {
        'id_pedido': 'VARCHAR(200)',
        'id_item_pedido': 'INTEGER',
        'id_produto': 'VARCHAR(200)',
        'id_vendedor': 'VARCHAR(200)',
        'data_limite_envio': 'TIMESTAMP',
        'preco': 'DECIMAL(10,2)',
        'valor_frete': 'DECIMAL(10,2)',
        'FOREIGN KEY (id_pedido)': ' REFERENCES dim_pedido(id_pedido)'
    }

    campos_pedido = {
        'id_pedido': 'VARCHAR(200) PRIMARY KEY',
        'id_cliente': 'VARCHAR(200)',
        'status_pedido': 'VARCHAR(50)',
        'timestamp_compra': 'TEXT',
        'timestamp_aprovacao': 'TEXT',
        'timestamp_entrega_transportadora': 'TEXT',
        'timestamp_entrega_cliente': 'TEXT',
        'data_estimada_entrega': 'TEXT',
        'FOREIGN KEY (id_cliente)': ' REFERENCES dim_cliente(id_cliente)'
    }

    campos_produto = {
        'id_produto': 'VARCHAR(200) PRIMARY KEY',
        'nome_categoria_produto': 'VARCHAR(255)',
        'comprimento_nome_produto': 'INTEGER',
        'comprimento_descricao_produto': 'INTEGER',
        'quantidade_fotos_produto': 'INTEGER',
        'peso_produto_g': 'DECIMAL(10,2)',
        'comprimento_produto_cm': 'DECIMAL(10,2)',
        'altura_produto_cm': 'DECIMAL(10,2)',
        'largura_produto_cm': 'DECIMAL(10,2)'
    }

    campos_pedidos_avaliacao = {
        'id_avaliacao': 'VARCHAR(200)',
        'id_pedido': 'VARCHAR(200)',
        'pontuacao_avaliacao': 'INTEGER',
        'titulo_comentario_avaliacao': 'VARCHAR(255)',
        'mensagem_comentario_avaliacao': 'TEXT',
        'data_criacao_avaliacao': 'TEXT',
        'timestamp_resposta_avaliacao': 'TEXT',
        'PRIMARY KEY ': '(id_avaliacao, id_pedido)',
        'FOREIGN KEY (id_pedido)': ' REFERENCES dim_pedido(id_pedido)'
    }

    campos_pedidos_pagamento = {
        'id_pedido': 'VARCHAR(200)',
        'parcelas': 'INTEGER',
        'tipo_pagamento': 'VARCHAR(50)',
        'parcelas_pagamento': 'INTEGER',
        'valor_pagamento': 'DECIMAL(10,2)',
        'FOREIGN KEY (id_pedido)': ' REFERENCES dim_pedido(id_pedido)'
    }

    campos_fato = {
        'id_pedido': 'VARCHAR(200)',
        'id_cliente': 'VARCHAR(200)',
        'status_pedido': 'VARCHAR(50)',
        'timestamp_compra': 'TEXT',
        'timestamp_aprovacao': 'TEXT',
        'timestamp_entrega_transportadora': 'TEXT',
        'timestamp_entrega_cliente': 'TEXT',
        'data_estimada_entrega': 'TEXT',
        'id_unico_cliente': 'VARCHAR(200)',
        'cod_postal_prefixo': 'INTEGER',
        'cidade_cliente': 'VARCHAR(100)',
        'estado_cliente': 'VARCHAR(50)',
        'id_item_pedido': 'INTEGER',
        'id_produto': 'VARCHAR(200)',
        'id_vendedor': 'VARCHAR(200)',
        'data_limite_envio': 'TIMESTAMP',
        'preco': 'DECIMAL(10,2)',
        'valor_frete': 'DECIMAL(10,2)',
        'nome_categoria_produto': 'VARCHAR(255)',
        'comprimento_nome_produto': 'INTEGER',
        'comprimento_descricao_produto': 'INTEGER',
        'quantidade_fotos_produto': 'INTEGER',
        'peso_produto_g': 'DECIMAL(10,2)',
        'comprimento_produto_cm': 'DECIMAL(10,2)',
        'altura_produto_cm': 'DECIMAL(10,2)',
        'largura_produto_cm': 'DECIMAL(10,2)',
        'parcelas': 'INTEGER',
        'tipo_pagamento': 'VARCHAR(50)',
        'parcelas_pagamento': 'INTEGER',
        'valor_pagamento': 'DECIMAL(10,2)',
        'id_avaliacao': 'VARCHAR(200)',
        'pontuacao_avaliacao': 'INTEGER',
        'titulo_comentario_avaliacao': 'VARCHAR(255)',
        'mensagem_comentario_avaliacao': 'TEXT',
        'data_criacao_avaliacao': 'TEXT',
        'timestamp_resposta_avaliacao': 'TEXT',
        'FOREIGN KEY (id_pedido)': ' REFERENCES dim_pedido(id_pedido)',
        'FOREIGN KEY (id_cliente)': ' REFERENCES dim_cliente(id_cliente)',
        'FOREIGN KEY (id_produto)': ' REFERENCES dim_produto(id_produto)',
    }

    query_cliente = criar_tabela('dim_cliente', campos_cliente)
    query_pedidos = criar_tabela('dim_pedido', campos_pedido)
    query_pedido_item = criar_tabela('dim_pedido_item', campos_pedido_item)
    query_produto = criar_tabela('dim_produto', campos_produto)
    query_pedido_pagamento = criar_tabela('dim_pedido_pagamento', campos_pedidos_pagamento)
    query_pedido_avaliacao = criar_tabela('dim_pedido_avaliacao', campos_pedidos_avaliacao)
    query_fato_pedido = criar_tabela('fato_pedido', campos_fato)

    insert_cliente = gerar_query_insert(df_cliente,'dim_cliente')
    insert_pedido = gerar_query_insert(df_pedidos, 'dim_pedido')
    insert_pedido_item = gerar_query_insert(df_pedido_item, 'dim_pedido_item')
    insert_produto = gerar_query_insert(df_produto, 'dim_produto')
    insert_pedido_pagamento = gerar_query_insert(df_pedidos_pagamento, 'dim_pedido_pagamento')
    insert_pedido_avaliacao = gerar_query_insert(df_pedidos_avaliacao, 'dim_pedido_avaliacao')
    insert_fato_pedido = gerar_query_insert(df_fato_pedidos, 'fato_pedido')
    
    queries = [query_cliente, query_pedidos, query_pedido_item, query_produto, query_pedido_pagamento, query_pedido_avaliacao
               ,query_fato_pedido, insert_cliente, insert_pedido, insert_produto, insert_pedido_item, insert_pedido_pagamento, insert_pedido_avaliacao,
                 insert_fato_pedido]

    cur = conn.cursor()

    try:
        [cur.execute(q) for q in queries]
        print('Queries executadas com sucesso.')
        conn.commit()
        print('Salvo no Banco de dados.')
    except psycopg2.errors.SyntaxError as e:
        print(f"[ERRO DE SINTAXE] Verifique sua sintaxe: {e.pgerror}")
    except psycopg2.errors.UndefinedTable as e:
        print(f"[TABELA INDEFINIDA] Verifique suas tabelas e relacoes: {e.pgerror}")
    finally:
        print("Conexão finalizada!")
        conn.close()