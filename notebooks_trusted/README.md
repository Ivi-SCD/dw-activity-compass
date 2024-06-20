<div align="center">

## Layer Trusted

O processo de construção da camada Trusted foi desenvolvido e documentado em cinco notebooks, 
cada um responsável pelo processamento de uma tabela específica (Porém também disponibilizarei um 
Script Python). As modificações e transformações necessárias para cada tabela foram realizadas 
nesses notebooks.

</div>

#

### Estrutura

> Obs: **CASO VA EXECUTAR OS NOTEBOOKS, É IMPORTANTE QUE OS NOTEBOOKS SEJAM EXECUTADOS NESTA ORDEM!!!!!**

Otherwise, caso vá executar diretamente o script python, os notebooks ficam apenas de exemplo para você entender como e porque eu tomei determinadas decisões de alterações no dataset: [processing_trusted.py](../etl_scripts/processing_trusted.py)

- **[customer_to_trusted.ipynb](./customer_to_trusted.ipynb)**: Processamento e carregamento do dataset Customer.
- **[payments_to_trusted.ipynb](./payments_to_trusted.ipynb)**: Processamento e carregamento do dataset Payment.
- **[reviews_to_trusted.ipynb](./reviews_to_trusted.ipynb)**: Processamento e carregamento do dataset Review.
- **[products_to_trusted.ipynb](./products_to_trusted.ipynb)**: Processamento e carregamento do dataset Product.
- **[itens_to_trusted.ipynb](./itens_to_trusted.ipynb)**: Processamento e carregamento do dataset Itens.

