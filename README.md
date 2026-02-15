
[![build status](https://img.shields.io/github/actions/workflow/status/VFLins/nflogic/python-package.yml?branch=main&label=tests&style=for-the-badge)](https://github.com/VFLins/nflogic/tree/main/tests)
[![test coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2FVFLins%2Fnflogic%2Frefs%2Fheads%2Fmain%2F.github%2Fcoverage.json&style=for-the-badge)](https://github.com/VFLins/nflogic/tree/main/tests)
[![code style](https://img.shields.io/badge/code%20style-black-black?style=for-the-badge)](https://black.readthedocs.io/)

![nflogic banner](./resources/gh_banner-nflogic.png)

- [Introdução](#introdução)
- [Instalando](#instalando)
- [Como usar](#como-usar)
  - [Processando arquivos dentro de uma pasta](#processando-arquivos-dentro-de-uma-pasta)
  - [Verificando erros](#verificando-erros)
  - [Corrigindo erros / atualizando o `nflogic`](#corrigindo-erros--atualizando-o-nflogic)
  - [Processando arquivos que já falharam](#processando-arquivos-que-já-falharam)
  

# Introdução

Este software permite processar os dados de muitos arquivos de notas fiscais eletrônicas
e transformá-los em um conjunto de dados prontos para ser analisado por um analista de
dados ou incorporado em uma aplicação para empresas.

> [!WARNING]
> Este projeto ainda está em fase experimental e deve ser usado com cautela, embora haja
> um esforço para tal, ainda não é possível validar os dados coletados com total clareza.


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

Aqui estão alguns exemplos simples para aproveitar as principais funcionalidades do
`nflogic` rapidamente.

## Processando arquivos dentro de uma pasta

A principal funcionalidade do `nflogic` está em processar documentos XML de notas fiscais
eletrônicas, para isso:

- **API**

  ```python
  import nflogic
  nflogic.parse_dir(dir_path="/full/path/to/directory", buy=False)
  ```

- **CLI**

  ```bash
  nflogic parse --ParseTo=seller "/full/path/to/directory"
  ```

Estes comandos são equivalentes, e vão registrar os dados obtidos no diretório informado
como vendas realizadas pelo vendedor que estiver informado no próprio documento. Caso
existam múltiplos vendedores, uma tabela será construída para cada um que for encontrado.

Os dados são armazenados em um banco de dados que fica junto ao local de instalação do
`nflogic`. Para saber onde está instalado, você pode usar este comando, caso tenha
instalado com `pip`:

```bash
pip show nflogic
```

ou este, caso tenha instalado com `pipx`:

```bash
pipx runpip nflogic show nflogic
```

Procure pela linha que começa com "Location:" entre no diretóiro informado, e a partir
de lá, o banco de dados será encontrado em "`nflogic/database/db.sqlite`".

## Verificando erros

Depois de processar muitos dados em massa, é possível que o `nflogic` encontre falhas em
alguns arquivos, dependendo da quantidade de notas fiscais processadas, você pode ter
muitos arquivos para analisar. O `nflogic` permite ver um resumo das falhas de
processamento dos arquivos, indicando as mensagens de erro emitidas junto com a etapa do
processamento em que a falha ocorreu.

O `nflogic` faz o melhor possível para identificar o nome do vendedor ou comprador da
nota, salva os erros em *caches* separados por nome. Quando não é possível, o erro é
registrado em um arquivo comum para todos os documentos em que o nome não pode ser
identificado, para listar os nomes dos arquivos de cache:

- **API**

  ```python
  import nflogic
  # obtenha uma lista com o nome dos arquivos de cache
  cachenames = nflogic.cache.get_cachenames()
  # se algum erro ja tiver sido registrado, mostre na tela (stdout) o nome do cache e o
  # resumo dos erros obtidos
  for cn in cachenames:
      error_df = nflogic.rebuild_errors(cachename=cn)
      summary = nflogic.summary_err_types(err_df=error_df)
      print(f"{cn}\n----", summary.to_string(), "\n", sep="\n")
  ```

> [!NOTE]
> Explicando um pouco o código acima:
> 1. A função `rebuild_errors()` vai tentar repetir o procedimento que falhou
>    anteriormente, registrando todas as informações pertinentes de falhas em um
>    [`pandas.DataFrame`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html);
> 2. A função `summary_err_types` agrupa os erros similares por tipo, retornando um
>    `pandas.DataFrame` mais conciso.

- **CLI**

  Veja os nomes de cache disponíveis com:

  ```bash
  nflogic cachenames
  ```
  
  Depois veja o resumo dos erros, substituindo `[CACHENAME]` por um dos nomes listados
  anteriormente:

  ```bash
  nflogic errors --summary [CACHENAME]
  ```
  
Usar a flag `--summary` faz com que retorne a tabela resumida dos erros agregados por
tipo, omití-la retornaria o conjunto de dados completo.
  
## Corrigindo erros / atualizando o `nflogic`

Os problemas de _parsing_ podem ser corrigidos localmente usando um editor de texto para
modificar o código fonte do `nflogic`. Por se tratar de um programa interpretado, e não
compilado, alterações no código são refletidas diretamente no comportamento da CLI
localmente.

Caso você tenha capacidade para entender o problema encontrado, e tenha uma sugestão para
alterar o código fonte do `nflogic`, considere abrir uma
[_issue_](https://github.com/VFLins/nflogic/issues) para informar sobre a existência do
problema. Se você também se sente capaz de resolver o problema encontrado, sinta-se livre
para propor uma [_pull request_](https://github.com/VFLins/nflogic/pulls) para que esta
mudança afete os demais usuários.

Se você não se identificou com nenhum dos casos citados acima, mantenha sua instalação do
`nflogic` atualizada, o problema que você enfrenta pode ser corrigido em alguma
atualização futura. Para atualizar a sua instalação:

- **Pelo `pip`**

  ```bash
  pip install --upgrade nflogic
  ```

- **Pelo `pipx`**

  ```bash
  pipx upgrade nflogic
  ```

Leia as últimas linhas do resultado retornado para saber se alguma nova versão foi
instalada, ou se você já está na versão mais recente.

## Processando arquivos que já falharam

Como já sabemos como [verificar erros](#verificando-erros), podemos pular para a parte em
que vemos como podemos fazer com que o `nflogic` tente processar novamente os arquivos que
já deram erro no passado:

- **API**

  ```python
  import nflogic
  nflogic.parse_cache(cachename="SOME_CACHE_NAME", full_parse=True)
  ```

Neste caso, `cachename` deve ser um dos nomes de cache retornados por
`nflogic.cache.get_cachenames()`, e `full_parse` indica se devem ser extraídos apenas os
metadados da nota fiscal, ou também as informações transacionais, como nomes dos produtos
ou valores unitários.

- **CLI**

  ```bash
  nflogic parse-cache [CACHENAME]
  ```

A versão de CLI atualmente sempre processa os dados dos documentos em cache usando
`full_parse=True`.
