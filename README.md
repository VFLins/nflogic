# Introdução

Transforme séries de Notas Fiscais Eletrônicas XML em conjuntos de dados prontos para
análise.

# Instalando

A maneira recomendada de instalar o `nflogic` como uma API é através do
[`PyPI`](https://pypi.org/project/nflogic/) usando o console do seu computador:

```bash
pip install nflogic
```

Caso queira usar como uma linha de comando, a maneira recomendada é primeiramente
instalar o `pipx` seguindo 
[estas instruções](https://packaging.python.org/pt-br/latest/guides/installing-stand-alone-command-line-tools/),
e depois usá-lo para instalar o `nflogic`:

```bash
pipx install nflogic
```

# Como usar

Aqui estão alguns exemplos simples para aproveitar as principais funcionalidades do `nflogic` rapidamente.

## Processando arquivos dentro de uma pasta

A principal funcionalidade do `nflogic` está em processar documentos XML de notas fiscais eletrônicas, para isso

- **API**
  ```python
  import nflogic
  nflogic.parse_dir(dir_path="/full/path/to/directory", buy=False)
  ```

- **CLI**
  ```bash
  nflogic parse --ParseTo=seller "/full/path/to/directory"
  ```

Estes comandos são equivalentes, e vão registrar os dados obtidos no diretório informado como vendas realizadas pelo vendedor que estiver informado no próprio documento. Caso existam múltiplos vendedores, uma tabela será construída para cada um que for encontrado.

Os dados são armazenados em um banco de dados que fica junto ao local de instalação do `nflogic`. Para saber onde está instalado, você pode usar este comando, caso tenha instalado com `pip`:

```bash
pip show nflogic
```

ou este, caso tenha instalado com `pipx`:

```bash
pipx runpip nflogic show nflogic
```

Procure pela linha que começa com "Location:" entre no diretóiro informado, e a partir de lá, o banco de dados será encontrado em "`nflogic/database/db.sqlite`".
