import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASS")

df_cliente = pd.read_csv('./output/pd/refined/dim_cliente.csv')
df_avaliacao = pd.read_csv('./output/pd/refined/dim_avaliacao.csv')
df_pagamento = pd.read_csv('./output/pd/refined/dim_pagamento.csv')
df_pedido = pd.read_csv('./output/pd/refined/dim_pedido.csv')
df_produto = pd.read_csv('./output/pd/refined/dim_produto.csv')
df_tempo_data = pd.read_csv('./output/pd/refined/dim_tempo_data.csv')
df_tempo_instante = pd.read_csv('./output/pd/refined/dim_tempo_instante.csv')
df_fato_pedidos = pd.read_csv('./output/pd/refined/fato_pedido.csv')


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
        'cliente_id': 'INTEGER PRIMARY KEY',
        'cliente_codigo_postal': 'VARCHAR(200)',
        'cliente_cidade': 'VARCHAR(50)',
        'cliente_estado': 'VARCHAR(2)',
    }

    campos_pedido = {
        'pedido_id': 'INTEGER PRIMARY KEY',
        'pedido_status': 'VARCHAR(20)',
    }

    campos_produto = {
        'produto_id': 'INTEGER PRIMARY KEY',
        'produto_comprimento_cm': 'DECIMAL(10,2)',
        'produto_altura_cm':'DECIMAL(10,2)',
        'produto_largura_cm': 'DECIMAL(10,2)',
        'produto_peso_g': 'DECIMAL(10,2)',
        'produto_qtd_fotos': 'INTEGER',
        'produto_categoria': 'VARCHAR(100)',

    }

    campos_avaliacao = {
        'avaliacao_id': 'INTEGER PRIMARY KEY',
        'avaliacao_titulo_comentario': 'VARCHAR(255)',
        'avaliacao_mensagem_comentario': 'TEXT',
    }

    campos_pagamento = {
        'pagamento_id': 'INTEGER PRIMARY KEY',
        'pagamento_tipo': 'VARCHAR(20)',
    }

    campos_tempo_data = {
        'tempo_data_id': 'INTEGER PRIMARY KEY',
        'ano':'INTEGER',
        'mes':'INTEGER',
        'dia':'INTEGER'
    }

    campos_tempo_instante = {
        'tempo_instante_id': 'INTEGER PRIMARY KEY',
        'hora':'INTEGER',
        'minuto':'INTEGER',
        'segundo':'INTEGER'
    }

    campos_fato_pedido = {
    'pedido_id': 'INTEGER',
    'cliente_id': 'INTEGER',
    'pagamento_id': 'INTEGER',
    'produto_id': 'INTEGER',
    'avaliacao_id': 'INTEGER',
    'pedido_compra_data_id': 'INTEGER',
    'pedido_compra_instante_id': 'INTEGER',
    'pedido_aprovacao_data_id': 'INTEGER',
    'pedido_aprovacao_instante_id': 'INTEGER',
    'pedido_entregue_operador_data_id': 'INTEGER',
    'pedido_entregue_operador_instante_id': 'INTEGER',
    'pedido_entregue_cliente_data_id': 'INTEGER',
    'pedido_entregue_cliente_instante_id': 'INTEGER',
    'pedido_entrega_estimada_data_id': 'INTEGER',
    'envio_limite_data_id': 'INTEGER',
    'envio_limite_instante_id': 'INTEGER',
    'avaliacao_criacao_data_id': 'INTEGER',
    'avaliacao_criacao_instante_id': 'INTEGER',
    'avaliacao_resposta_instante_id': 'INTEGER',
    'avaliacao_pontuacao': 'INTEGER',
    'pagamento_valor': 'DECIMAL',
    'pagamento_formas_distintas': 'INTEGER',
    'pagamento_parcelamentos': 'INTEGER',
    'preco': 'DECIMAL',
    'frete_valor': 'DECIMAL',
    'FOREIGN KEY (pedido_id)': ' REFERENCES dim_pedido(pedido_id)',
    'FOREIGN KEY (cliente_id)': ' REFERENCES dim_cliente(cliente_id)',
    'FOREIGN KEY (pagamento_id)': ' REFERENCES dim_pagamento(pagamento_id)',
    'FOREIGN KEY (produto_id)': ' REFERENCES dim_produto(produto_id)',
    'FOREIGN KEY (avaliacao_id)': ' REFERENCES dim_avaliacao(avaliacao_id)',
    'FOREIGN KEY (pedido_compra_data_id)': ' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (pedido_compra_instante_id)':' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (pedido_aprovacao_data_id)':' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (pedido_aprovacao_instante_id)':' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (pedido_entregue_operador_data_id)':' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (pedido_entregue_operador_instante_id)':' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (pedido_entregue_cliente_data_id)':' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (pedido_entregue_cliente_instante_id)':' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (pedido_entrega_estimada_data_id)': ' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (envio_limite_data_id)': ' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (envio_limite_instante_id)': ' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (avaliacao_criacao_data_id)': ' REFERENCES dim_tempo_data(tempo_data_id)',
    'FOREIGN KEY (avaliacao_criacao_instante_id)': ' REFERENCES dim_tempo_instante(tempo_instante_id)',
    'FOREIGN KEY (avaliacao_resposta_instante_id)': ' REFERENCES dim_tempo_instante(tempo_instante_id)',
}


    query_cliente = criar_tabela('dim_cliente', campos_cliente)
    query_pedidos = criar_tabela('dim_pedido', campos_pedido)
    query_produto = criar_tabela('dim_produto', campos_produto)
    query_avaliacao = criar_tabela('dim_avaliacao', campos_avaliacao)
    query_pagamento = criar_tabela('dim_pagamento', campos_pagamento)
    query_tempo_data = criar_tabela('dim_tempo_data', campos_tempo_data)
    query_tempo_instante = criar_tabela('dim_tempo_instante', campos_tempo_instante)
    query_fato_pedido = criar_tabela('fato_pedido', campos_fato_pedido)

    insert_cliente = gerar_query_insert(df_cliente,'dim_cliente')
    insert_pedido = gerar_query_insert(df_pedido, 'dim_pedido')
    insert_produto = gerar_query_insert(df_produto, 'dim_produto')
    insert_avaliacao = gerar_query_insert(df_avaliacao, 'dim_avaliacao')
    insert_pagamento = gerar_query_insert(df_pagamento, 'dim_pagamento')
    insert_tempo_data = gerar_query_insert(df_tempo_data, 'dim_tempo_data')
    insert_tempo_instante = gerar_query_insert(df_tempo_instante, 'dim_tempo_instante')
    insert_fato_pedido = gerar_query_insert(df_fato_pedidos, 'fato_pedido')
    
    queries = [query_cliente, query_pedidos, query_produto, query_avaliacao, query_pagamento, query_tempo_data
               ,query_tempo_instante, query_fato_pedido, insert_cliente, insert_pedido, insert_produto, insert_avaliacao, insert_pagamento, insert_tempo_data,
                 insert_tempo_instante, insert_fato_pedido]

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