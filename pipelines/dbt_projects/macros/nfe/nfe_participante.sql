{#
  Chave de versão do participante = md5(doc | hash dos atributos cadastrais).
  A MESMA fórmula é usada no histórico (dimensão) e na fato: a nota carrega os atributos que o
  contribuinte tinha na emissão, então a versão certa sai da própria nota, sem join por intervalo
  de datas (exato por construção, inclusive para notas que chegam atrasadas).
  prefixo: 'emit' ou 'dest' (colunas da silver.nfe_nota)
#}
{% macro nfe_hash_atributos(nome, ie, uf, cod_municipio, municipio, logradouro, bairro, cep) -%}
md5(concat_ws('|', {{ nome }}, {{ ie }}, {{ uf }}, {{ cod_municipio }}, {{ municipio }}, {{ logradouro }}, {{ bairro }}, {{ cep }}))
{%- endmacro %}

{% macro nfe_sk_participante(prefixo, alias='n') -%}
    {%- set doc = alias ~ '.' ~ ('emit_cnpj' if prefixo == 'emit' else 'dest_doc') -%}
md5({{ doc }} || '|' || {{ nfe_hash_atributos(
        alias ~ '.' ~ prefixo ~ '_nome', alias ~ '.' ~ prefixo ~ '_ie', alias ~ '.' ~ prefixo ~ '_uf',
        alias ~ '.' ~ prefixo ~ '_cod_municipio', alias ~ '.' ~ prefixo ~ '_municipio',
        alias ~ '.' ~ prefixo ~ '_logradouro', alias ~ '.' ~ prefixo ~ '_bairro', alias ~ '.' ~ prefixo ~ '_cep') }})
{%- endmacro %}
