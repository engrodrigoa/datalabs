#!/usr/bin/env python3
"""
Simulador de emissão de NF-e (modelo 55, layout 4.00 + grupos IBS/CBS/IS — reforma tributária).

Papel no lab: é o "sistema de origem". Escreve XMLs numa pasta de chegada (inbox) como a SEFAZ
receberia; todo o processamento a partir daí é SQL + dbt (ver docs/nfe/README.md).

Realismo:
  * estrutura validada contra o XSD oficial (leiauteNFe_v4.00 + DFeTiposBasicos_v1.00) — opção --xsd
  * cadastro ESTÁVEL de contribuintes (mesmo --seed-cadastro => mesmas empresas em toda execução),
    volume concentrado (poucos emitentes respondem pela maior parte das notas)
  * alterações cadastrais datadas (razão social / endereço) -> alimentam a dimensão SCD2
  * perspectiva da SEFAZ-GO: emitente em GO, ou emitente de outra UF com destino GO
  * CNPJ alfanumérico (IN RFB 2.229/2024) para parte dos contribuintes, já aceito pelo XSD
  * 2026 = ano-teste: CBS 0,9% / IBS UF 0,1%; base IBS/CBS = vProd - ICMS - PIS - COFINS
  * escrita atômica (.tmp -> .xml): sensor nunca vê arquivo pela metade

Anomalias (somente as plausíveis em nota autorizada / no transporte):
  --defect-rate   inconsistência de negócio: total CBS ≠ Σ itens, vBC total ≠ Σ itens, cClassTrib×CST
  --dup-rate      reenvio: o mesmo XML chega de novo com outro nome de arquivo
  --corrupt-rate  arquivo truncado no transporte (XML malformado -> rejeitado na ingestão)

Dados 100% fictícios. Somente códigos públicos (IBGE, NCM, CFOP, CST) são reais.

Exemplos:
  python scripts/nfe/gen_nfe.py --inbox datasource/nfe/inbox --n 500
  python scripts/nfe/gen_nfe.py --inbox datasource/nfe/inbox --n 200 --lotes 10 --intervalo 30
  python scripts/nfe/gen_nfe.py --inbox /tmp/x --n 2000 --data-ini 2026-01-01 --data-fim 2026-09-30 \\
         --defect-rate 0.02 --dup-rate 0.01 --corrupt-rate 0.005 --manifesto /tmp/manifesto.csv
"""
from __future__ import annotations

import argparse
import base64
import csv
import os
import random
import string
import time
import zlib
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
import xml.etree.ElementTree as ET

NS = "http://www.portalfiscal.inf.br/nfe"
DS = "http://www.w3.org/2000/09/xmldsig#"
D = Decimal
P_IBSUF, P_IBSMUN, P_CBS = D("0.10"), D("0.00"), D("0.90")  # alíquotas-teste 2026

# ------------------------------------------------------------------------------ referências públicas
# UF -> (cUF IBGE, [(cMun IBGE, nome)])
UFS = {
    "GO": ("52", [("5208707", "Goiania"), ("5201405", "Aparecida de Goiania"), ("5201108", "Anapolis"),
                  ("5218805", "Rio Verde"), ("5205109", "Catalao"), ("5211909", "Jatai"),
                  ("5211503", "Itumbiara"), ("5209101", "Goiatuba"), ("5212501", "Luziania"),
                  ("5208004", "Formosa"), ("5204508", "Caldas Novas"), ("5213103", "Mineiros"),
                  ("5206206", "Cristalina"), ("5213806", "Morrinhos"), ("5220454", "Senador Canedo"),
                  ("5221403", "Trindade"), ("5209705", "Hidrolandia")]),
    "SP": ("35", [("3550308", "Sao Paulo"), ("3509502", "Campinas"), ("3543402", "Ribeirao Preto")]),
    "MG": ("31", [("3106200", "Belo Horizonte"), ("3170206", "Uberlandia"), ("3170107", "Uberaba")]),
    "MT": ("51", [("5103403", "Cuiaba"), ("5107602", "Rondonopolis")]),
    "MS": ("50", [("5002704", "Campo Grande")]),
    "DF": ("53", [("5300108", "Brasilia")]),
    "BA": ("29", [("2927408", "Salvador"), ("2919553", "Luis Eduardo Magalhaes")]),
    "PR": ("41", [("4106902", "Curitiba")]),
    "TO": ("17", [("1721000", "Palmas")]),
}
P_ICMS_INTERNA = {"GO": D("19"), "SP": D("18"), "MG": D("18"), "MT": D("17"), "MS": D("17"),
                  "DF": D("20"), "BA": D("20.5"), "PR": D("19.5"), "TO": D("20")}
SUL_SUDESTE = {"SP", "MG", "PR", "RJ", "RS", "SC"}


def p_icms_inter(uf_orig: str, uf_dest: str) -> D:
    """7% de S/SE (exceto ES) para N/NE/CO e ES; 12% nos demais casos (produto nacional)."""
    return D("7") if uf_orig in SUL_SUDESTE and uf_dest not in SUL_SUDESTE else D("12")


# segmento -> catálogo (xProd, NCM, uCom, faixa de preço, perfil fiscal IBS/CBS)
#   perfil: normal (CST 000) | agro_dif (CST 515, diferimento + redução) | agro_red (CST 200, redução 60%)
CATALOGO = {
    "agro_insumos": [
        ("FERTILIZANTE NPK 04-14-08 GRANULADO", "31052000", "TO", (1800, 3200), "agro_dif"),
        ("FERTILIZANTE MAP 11-52-00", "31054000", "TO", (2500, 4200), "agro_dif"),
        ("SUPERFOSFATO SIMPLES", "31031900", "TO", (1200, 2000), "agro_dif"),
        ("CLORETO DE POTASSIO 60%", "31042010", "TO", (2200, 3500), "agro_red"),
        ("HERBICIDA GLIFOSATO 480 - BD 20 L", "38089329", "BD20", (300, 650), "agro_red"),
        ("INSETICIDA PIRETROIDE - GL 5 L", "38086290", "GL5", (400, 950), "agro_red"),
        ("FUNGICIDA TRIAZOL - GL 5 L", "38089299", "GL5", (350, 900), "agro_red"),
        ("SEMENTE DE SOJA TRATADA - SC 40 KG", "12011000", "SC", (250, 480), "agro_red"),
    ],
    "atacado_alimentos": [
        ("ARROZ BENEFICIADO TIPO 1 5KG", "10063021", "FD", (90, 160), "normal"),
        ("FEIJAO CARIOCA TIPO 1 1KG", "07133399", "FD", (60, 140), "normal"),
        ("OLEO DE SOJA REFINADO 900ML", "15079011", "CX", (100, 190), "normal"),
        ("ACUCAR CRISTAL 5KG", "17019900", "FD", (80, 150), "normal"),
        ("CAFE TORRADO E MOIDO 500G", "09012100", "CX", (180, 420), "normal"),
    ],
    "bebidas": [
        ("REFRIGERANTE COLA 2L", "22021000", "PCT", (40, 70), "normal"),
        ("CERVEJA LATA 350ML", "22030000", "CX", (45, 90), "normal"),
        ("AGUA MINERAL SEM GAS 500ML", "22011000", "PCT", (10, 25), "normal"),
    ],
    "varejo_eletro": [
        ("FONE DE OUVIDO BLUETOOTH", "85183000", "UN", (40, 300), "normal"),
        ("CABO USB-C 1M", "85444200", "UN", (10, 60), "normal"),
        ("SMARTPHONE 128GB", "85171300", "UN", (900, 3500), "normal"),
        ("NOTEBOOK 14 POL", "84713012", "UN", (2200, 6500), "normal"),
        ("JOGO DE PANELAS ANTIADERENTE 5 PCS", "73239300", "CJ", (120, 450), "normal"),
        ("VARAL DE CHAO DOBRAVEL ACO", "73239900", "UN", (25, 180), "normal"),
    ],
}
SEGMENTO_PESO = {"agro_insumos": 35, "atacado_alimentos": 30, "bebidas": 15, "varejo_eletro": 20}
IS_NCM = {"22021000", "22030000"}  # sujeitos ao Imposto Seletivo (simulação --com-is)

NOMES_PJ = ["AGRO", "COMERCIAL", "DISTRIBUIDORA", "INDUSTRIA", "ATACADO", "CEREAIS", "INSUMOS", "SUPERMERCADO"]
SUFIXOS_PJ = ["ALFA", "BETA", "GAMA", "DELTA", "SIGMA", "OMEGA", "CERRADO", "PLANALTO", "HORIZONTE", "VALE",
              "ARAGUAIA", "PARANAIBA", "TOCANTINS", "PIRENEUS", "SERRA DOURADA"]
PRENOMES = ["ANA", "JOAO", "MARIA", "PEDRO", "LUCAS", "JULIA", "CARLOS", "FERNANDA", "PAULO", "BEATRIZ"]
SOBRENOMES = ["SILVA", "SOUZA", "OLIVEIRA", "PEREIRA", "COSTA", "ALMEIDA", "RIBEIRO", "MARTINS"]
LOGR = ["RUA", "AVENIDA", "RODOVIA", "ESTRADA", "ALAMEDA"]
BAIRROS = ["CENTRO", "SETOR INDUSTRIAL", "ZONA RURAL", "JARDIM AMERICA", "SETOR BUENO", "DISTRITO AGROINDUSTRIAL"]
SISTEMAS = ["ERP TESTE 1.0", "EMISSOR SIM 4.2", "SISTEMA XYZ 373.3", "SAP SIMULADO"]
ALNUM = string.digits + string.ascii_uppercase


# ------------------------------------------------------------------------------ utilidades
def q(x, casas=2) -> D:
    return D(x).quantize(D(1).scaleb(-casas), rounding=ROUND_HALF_UP)


def fmt(x, casas=2) -> str:
    return f"{q(x, casas):f}"


def _val(c: str) -> int:
    """Valor do caractere no cálculo do DV: ASCII - 48 (regra do CNPJ alfanumérico; dígitos inalterados)."""
    return ord(c) - 48


def dv_mod11(corpo: str, pesos_max: int = 9) -> str:
    """Módulo 11, pesos 2..pesos_max da direita p/ esquerda. Resto < 2 -> 0."""
    soma, p = 0, 2
    for ch in reversed(corpo):
        soma += _val(ch) * p
        p = 2 if p == pesos_max else p + 1
    r = soma % 11
    return "0" if r < 2 else str(11 - r)


def gera_cnpj(rng: random.Random, alfanumerico: bool = False) -> str:
    raiz = "".join(rng.choice(ALNUM if alfanumerico else string.digits) for _ in range(8))
    corpo = raiz + "0001"
    d1 = dv_mod11(corpo)
    return corpo + d1 + dv_mod11(corpo + d1)


def gera_cpf(rng: random.Random) -> str:
    n = [rng.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        s = sum(a * b for a, b in zip(n, range(len(n) + 1, 1, -1)))
        n.append(0 if s % 11 < 2 else 11 - s % 11)
    return "".join(map(str, n))


def monta_chave(cuf: str, dt: datetime, cnpj: str, serie: int, nnf: int, cnf: str) -> str:
    c43 = f"{cuf}{dt:%y%m}{cnpj}55{serie:03d}{nnf:09d}1{cnf}"
    return c43 + dv_mod11(c43)


def sub(parent, tag, text=None, ns=NS, **attrs):
    e = ET.SubElement(parent, f"{{{ns}}}{tag}", {k: str(v) for k, v in attrs.items()})
    if text is not None:
        e.text = str(text)
    return e


def subs(parent, pares):
    for t, v in pares:
        if v is not None:
            sub(parent, t, v)


# ------------------------------------------------------------------------------ cadastro estável
@dataclass(frozen=True)
class Participante:
    tipo: str            # CNPJ | CPF
    doc: str
    nome: str
    uf: str
    cmun: str
    xmun: str
    lgr: str
    nro: str
    bairro: str
    cep: str
    ie: str | None
    segmento: str | None = None


@dataclass
class Contribuinte:
    base: Participante
    peso: float
    mudanca_em: date | None = None            # data da alteração cadastral (SCD2)
    depois: Participante | None = None
    serie: int = 1
    prox_nnf: int = field(default=1)

    def em(self, d: date) -> Participante:
        return self.depois if self.mudanca_em and d >= self.mudanca_em else self.base


def _endereco(rng, uf):
    cmun, xmun = rng.choice(UFS[uf][1])
    return dict(uf=uf, cmun=cmun, xmun=xmun, lgr=f"{rng.choice(LOGR)} {rng.randint(1, 99)}",
                nro=rng.choice(["SN", "S/N", str(rng.randint(1, 3000))]), bairro=rng.choice(BAIRROS),
                cep=f"{rng.randint(10000000, 99999999)}")


def _pj(rng, uf, segmento=None, pct_alfa=0.0):
    nome = f"{rng.choice(NOMES_PJ)} {rng.choice(SUFIXOS_PJ)} {rng.choice(['LTDA', 'S.A.', 'LTDA', 'LTDA'])}"
    return Participante("CNPJ", gera_cnpj(rng, rng.random() < pct_alfa), nome, ie=str(rng.randint(10**8, 10**9 - 1)),
                        segmento=segmento, **_endereco(rng, uf))


def _pf(rng, uf, produtor_rural=False):
    nome = f"{rng.choice(PRENOMES)} {rng.choice(SOBRENOMES)} {rng.choice(SOBRENOMES)}"
    return Participante("CPF", gera_cpf(rng), nome, ie=str(rng.randint(10**8, 10**9 - 1)) if produtor_rural else None,
                        **_endereco(rng, uf))


class Cadastro:
    """Determinístico por seed: rodar o gerador várias vezes produz notas das MESMAS empresas."""

    def __init__(self, seed: int, n_emit: int, n_dest_pj: int, n_dest_pf: int, pct_alfa: float, pct_mudanca: float):
        # um gerador aleatório POR ENTIDADE: o contribuinte nº k é sempre o mesmo, mesmo que
        # outros parâmetros (quantidade, % alfanumérico) mudem entre execuções
        def r(papel: str, k: int) -> random.Random:
            return random.Random(f"{seed}-{papel}-{k}")

        ufs_fora = [u for u in UFS if u != "GO"]
        segs, pesos_seg = list(SEGMENTO_PESO), list(SEGMENTO_PESO.values())
        self.emitentes: list[Contribuinte] = []
        for rank in range(1, n_emit + 1):
            rng = r("emit", rank)
            uf = "GO" if rng.random() < 0.75 else rng.choice(ufs_fora)
            base = _pj(rng, uf, rng.choices(segs, weights=pesos_seg)[0], pct_alfa)
            c = Contribuinte(base, peso=1 / rank ** 1.1, serie=rng.choice([1, 1, 1, 2, 4]), prox_nnf=rng.randint(1, 50000))
            if rng.random() < pct_mudanca:   # alteração cadastral em algum dia de 2026
                c.mudanca_em = date(2026, 1, 1) + timedelta(days=rng.randint(30, 300))
                if rng.random() < 0.5:
                    c.depois = replace(base, nome=f"{base.nome.rsplit(' ', 1)[0]} {rng.choice(SUFIXOS_PJ)} LTDA")
                else:
                    c.depois = replace(base, **_endereco(rng, uf))
            self.emitentes.append(c)
        self.pesos_emit = [c.peso for c in self.emitentes]
        self.dest_pj = []
        for k in range(n_dest_pj):
            rng = r("dest_pj", k)
            self.dest_pj.append(_pj(rng, "GO" if rng.random() < 0.7 else rng.choice(ufs_fora), pct_alfa=pct_alfa))
        self.dest_pf = []
        for k in range(n_dest_pf):
            rng = r("dest_pf", k)
            self.dest_pf.append(_pf(rng, "GO" if rng.random() < 0.6 else rng.choice(ufs_fora),
                                    produtor_rural=rng.random() < 0.3))
        self.produtores = [p for p in self.dest_pf if p.ie]

    def emitente(self, rng) -> Contribuinte:
        return rng.choices(self.emitentes, weights=self.pesos_emit)[0]

    def destinatario(self, rng, emit: Participante) -> Participante:
        """Perspectiva SEFAZ-GO: se o emitente é de fora, o destinatário é de GO."""
        def ok(p):
            return p.doc != emit.doc and (emit.uf == "GO" or p.uf == "GO")
        if emit.segmento == "agro_insumos" and rng.random() < 0.6:
            pool = self.produtores
        elif emit.segmento == "varejo_eletro":
            pool = self.dest_pf
        else:
            pool = self.dest_pj
        for _ in range(50):
            p = rng.choice(pool)
            if ok(p):
                return p
        return next(p for p in self.dest_pj if ok(p))


# ------------------------------------------------------------------------------ nota
TIPOS = {  # natOp, tpNF, finNFe, CFOP (interno, interestadual)
    "venda":     ("VENDA DE MERCADORIA", "1", "1", ("5102", "6102")),
    "venda_ind": ("VENDA PRODUCAO DO ESTABELECIMENTO", "1", "1", ("5101", "6101")),
    "devolucao": ("DEVOLUCAO DE VENDA", "0", "4", ("1202", "2202")),
}


def _endereco_xml(parent, tag, p: Participante):
    e = sub(parent, tag)
    subs(e, [("xLgr", p.lgr), ("nro", p.nro), ("xBairro", p.bairro), ("cMun", p.cmun), ("xMun", p.xmun),
             ("UF", p.uf), ("CEP", p.cep), ("cPais", "1058"), ("xPais", "BRASIL")])


def gerar_nota(rng: random.Random, cad: Cadastro, dt: datetime, args, defeito: str = ""):
    contrib = cad.emitente(rng)
    emit = contrib.em(dt.date())
    dest = cad.destinatario(rng, emit)
    tipo = rng.choices(list(TIPOS), weights=[55, 30 if emit.segmento == "agro_insumos" else 5, 10])[0]
    natop, tpnf, finnfe, cfops = TIPOS[tipo]
    interestadual = emit.uf != dest.uf
    consumidor_final = dest.tipo == "CPF" and not dest.ie
    perfil_dec = rng.choice([2, 4])        # emissores variam casas decimais de alíquota
    zero_curto = rng.random() < 0.2        # alguns escrevem "0" em vez de "0.00" nos totais

    contrib.prox_nnf += rng.randint(1, 3)
    nnf = contrib.prox_nnf % 999_999_999 or 1
    cnf = f"{rng.randint(0, 99_999_999):08d}"
    cuf = UFS[emit.uf][0]
    chave = monta_chave(cuf, dt, emit.doc, contrib.serie, nnf, cnf)
    dh = dt.strftime("%Y-%m-%dT%H:%M:%S-03:00")

    nfe = ET.Element(f"{{{NS}}}NFe")
    inf = sub(nfe, "infNFe", Id=f"NFe{chave}", versao="4.00")

    ide = sub(inf, "ide")
    subs(ide, [("cUF", cuf), ("cNF", cnf), ("natOp", natop), ("mod", "55"), ("serie", contrib.serie), ("nNF", nnf),
               ("dhEmi", dh), ("dhSaiEnt", dh), ("tpNF", tpnf), ("idDest", "2" if interestadual else "1"),
               ("cMunFG", emit.cmun), ("tpImp", "1"), ("tpEmis", "1"), ("cDV", chave[-1]), ("tpAmb", "2"),
               ("finNFe", finnfe), ("indFinal", "1" if consumidor_final else "0"),
               ("indPres", "2" if emit.segmento == "varejo_eletro" else rng.choice(["1", "9"])),
               ("indIntermed", "0"), ("procEmi", "0"), ("verProc", rng.choice(SISTEMAS))])
    ref_chave = None
    if tipo == "devolucao":
        dt_ref = dt - timedelta(days=rng.randint(5, 60))
        ref_chave = monta_chave(cuf, dt_ref, emit.doc, contrib.serie, max(1, nnf - rng.randint(10, 500)),
                                f"{rng.randint(0, 99_999_999):08d}")
        sub(sub(ide, "NFref"), "refNFe", ref_chave)

    em = sub(inf, "emit")
    subs(em, [("CNPJ", emit.doc), ("xNome", emit.nome), ("xFant", emit.nome)])
    _endereco_xml(em, "enderEmit", emit)
    subs(em, [("IE", emit.ie), ("CRT", "3")])
    de = sub(inf, "dest")
    subs(de, [(dest.tipo, dest.doc), ("xNome", dest.nome)])
    _endereco_xml(de, "enderDest", dest)
    subs(de, [("indIEDest", "1" if dest.ie else "9"), ("IE", dest.ie)])

    cfop = cfops[1] if interestadual else cfops[0]
    tot = {k: D(0) for k in ["vBC", "vICMS", "vICMSDeson", "vProd", "vIPI", "vPIS", "vCOFINS", "vIS",
                             "vBCIBSCBS", "vIBSUF", "vIBSMun", "vCBS", "vDifUF", "vDifMun", "vDifCBS",
                             "vDevUF", "vDevMun", "vDevCBS"]}
    linhas = []
    catalogo = CATALOGO[emit.segmento]
    n_itens = rng.choices([1, 2, 3, 4, 5, 8, 15], weights=[35, 20, 15, 10, 10, 6, 4])[0]
    for n in range(1, n_itens + 1):
        xprod, ncm, ucom, (lo, hi), perfil = rng.choice(catalogo)
        qtd = q(D(rng.uniform(1, 40)), 4) if ucom == "TO" else D(rng.randint(1, 60))
        vun = q(D(rng.uniform(lo, hi)), 10)
        vprod = q(qtd * vun)

        det = sub(inf, "det", nItem=n)
        p = sub(det, "prod")
        ean = "SEM GTIN" if perfil != "normal" else f"789{rng.randint(10**9, 10**10 - 1)}"
        subs(p, [("cProd", f"{zlib.crc32((emit.doc + xprod).encode()) % 99999:05d}"), ("cEAN", ean), ("xProd", xprod),
                 ("NCM", ncm), ("cBenef", "GO822018" if perfil != "normal" and emit.uf == "GO" else None),
                 ("CFOP", cfop), ("uCom", ucom), ("qCom", fmt(qtd, 4)), ("vUnCom", fmt(vun, 10)),
                 ("vProd", fmt(vprod)), ("cEANTrib", ean), ("uTrib", ucom), ("qTrib", fmt(qtd, 4)),
                 ("vUnTrib", fmt(vun, 10)), ("indTot", "1")])
        imp = sub(det, "imposto")

        # ICMS
        p_icms = p_icms_inter(emit.uf, dest.uf) if interestadual else P_ICMS_INTERNA[emit.uf]
        icms = sub(imp, "ICMS")
        vicms = vdeson = vbc_icms = D(0)
        if perfil == "agro_red" and rng.random() < 0.4:
            subs(sub(icms, "ICMS40"), [("orig", "0"), ("CST", "40")])
        elif perfil != "normal":
            pred = D(rng.choice(["78.95", "60.00", "30.00"]))
            vbc_icms = q(vprod * (1 - pred / 100))
            vicms = q(vbc_icms * p_icms / 100)
            vdeson = q(vprod * p_icms / 100) - vicms
            subs(sub(icms, "ICMS20"), [("orig", "0"), ("CST", "20"), ("modBC", "3"), ("pRedBC", fmt(pred, 4)),
                                       ("vBC", fmt(vbc_icms)), ("pICMS", fmt(p_icms, 4)), ("vICMS", fmt(vicms)),
                                       ("vICMSDeson", fmt(vdeson)), ("motDesICMS", "3"), ("indDeduzDeson", "0")])
        else:
            vbc_icms = vprod
            vicms = q(vbc_icms * p_icms / 100)
            subs(sub(icms, "ICMS00"), [("orig", "0"), ("CST", "00"), ("modBC", "3"), ("vBC", fmt(vbc_icms)),
                                       ("pICMS", fmt(p_icms, 4)), ("vICMS", fmt(vicms))])

        # IPI (só tributado para eletro; demais NT)
        ipi = sub(imp, "IPI")
        sub(ipi, "cEnq", "999")
        vipi = D(0)
        if emit.segmento == "varejo_eletro" and rng.random() < 0.4:
            vipi = q(vprod * D("0.065"))
            subs(sub(ipi, "IPITrib"), [("CST", "50" if tpnf == "1" else "00"), ("vBC", fmt(vprod)),
                                       ("pIPI", "6.5000"), ("vIPI", fmt(vipi))])
        else:
            sub(sub(ipi, "IPINT"), "CST", "53" if tpnf == "1" else "03")

        # PIS/COFINS (base sem ICMS)
        vpis = vcof = D(0)
        for trib, aliq in (("PIS", D("1.65")), ("COFINS", D("7.60"))):
            g = sub(imp, trib)
            if perfil != "normal":
                sub(sub(g, f"{trib}NT"), "CST", "06")
                continue
            vb = vprod - vicms
            v = q(vb * aliq / 100)
            subs(sub(g, f"{trib}Aliq" if tpnf == "1" else f"{trib}Outr"),
                 [("CST", "01" if tpnf == "1" else "98"), ("vBC", fmt(vb)), (f"p{trib}", fmt(aliq, 4)), (f"v{trib}", fmt(v))])
            if trib == "PIS":
                vpis = v
            else:
                vcof = v

        if interestadual and consumidor_final:  # DIFAL
            subs(sub(imp, "ICMSUFDest"),
                 [("vBCUFDest", fmt(vprod)), ("vBCFCPUFDest", fmt(vprod)), ("pFCPUFDest", "0.0000"),
                  ("pICMSUFDest", fmt(P_ICMS_INTERNA[dest.uf], 4)), ("pICMSInter", fmt(p_icms)),
                  ("pICMSInterPart", "100"), ("vFCPUFDest", "0.00"), ("vICMSUFDest", "0.00"), ("vICMSUFRemet", "0.00")])

        vis = D(0)
        if args.com_is and ncm in IS_NCM:
            vis = q(vprod * D("0.10"))
            subs(sub(imp, "IS"), [("CSTIS", "000"), ("cClassTribIS", "000001"), ("vBCIS", fmt(vprod)),
                                  ("pIS", fmt(10, 4)), ("vIS", fmt(vis))])

        # IBS / CBS
        vbc = vprod - vicms - vpis - vcof
        if perfil == "agro_dif":
            cst, cclass, pred_aliq, pdif = "515", "515001", D(60), D(100)
        elif perfil == "agro_red":
            cst, cclass, pred_aliq, pdif = "200", "200038", D(60), None
        else:
            cst, cclass, pred_aliq, pdif = "000", "000001", None, None
        if defeito == "cclasstrib_incoerente" and n == 1:
            cclass = {"000": "200038", "200": "000001", "515": "200038"}[cst]
        devol = tipo == "devolucao"

        ibs = sub(imp, "IBSCBS")
        subs(ibs, [("CST", cst), ("cClassTrib", cclass)])
        gi = sub(ibs, "gIBSCBS")
        sub(gi, "vBC", fmt(vbc))
        valores = {}
        for grp, tag_p, tag_v, aliq, suf in (("gIBSUF", "pIBSUF", "vIBSUF", P_IBSUF, "UF"),
                                             ("gIBSMun", "pIBSMun", "vIBSMun", P_IBSMUN, "Mun"),
                                             ("gCBS", "pCBS", "vCBS", P_CBS, "CBS")):
            if grp == "gCBS":
                sub(gi, "vIBS", fmt(valores["vIBSUF"] + valores["vIBSMun"]))
            g = sub(gi, grp)
            sub(g, tag_p, fmt(aliq, perfil_dec))
            p_efet = q(aliq * (1 - pred_aliq / 100), 4) if pred_aliq is not None else aliq
            bruto = q(vbc * p_efet / 100)
            valor, vdif, vdev = bruto, D(0), D(0)
            if pdif is not None:
                vdif = q(bruto * pdif / 100)
                valor = bruto - vdif
                subs(sub(g, "gDif"), [("pDif", fmt(pdif, perfil_dec)), ("vDif", fmt(vdif))])
            if devol and pdif is None and pred_aliq is not None:
                vdev, valor = bruto, D(0)
                sub(sub(g, "gDevTrib"), "vDevTrib", fmt(vdev))
            if pred_aliq is not None:
                subs(sub(g, "gRed"), [("pRedAliq", fmt(pred_aliq, perfil_dec)), ("pAliqEfet", fmt(p_efet, perfil_dec))])
            sub(g, tag_v, fmt(valor))
            valores[tag_v] = valor
            tot[f"vDif{suf}"] += vdif
            tot[f"vDev{suf}"] += vdev

        if devol:
            idv = sub(det, "impostoDevol")
            sub(idv, "pDevol", "100.00")
            sub(sub(idv, "IPI"), "vIPIDevol", "0.00")
        sub(det, "infAdProd", f"LOTE: {rng.randint(1000, 9999)}-{rng.randint(20, 26)} - FAB.: {dt:%d-%m-%Y}")
        if rng.random() < 0.7:
            sub(det, "vItem", fmt(vprod + vipi))

        for k, v in (("vBC", vbc_icms), ("vICMS", vicms), ("vICMSDeson", vdeson), ("vProd", vprod), ("vIPI", vipi),
                     ("vPIS", vpis), ("vCOFINS", vcof), ("vIS", vis), ("vBCIBSCBS", vbc), ("vIBSUF", valores["vIBSUF"]),
                     ("vIBSMun", valores["vIBSMun"]), ("vCBS", valores["vCBS"])):
            tot[k] += v
        linhas.append((n, ncm, cst, cclass, vprod, vbc, valores["vCBS"]))

    vnf = tot["vProd"] + tot["vIPI"]
    if defeito == "tot_cbs_divergente":
        tot["vCBS"] += D("0.05")
    elif defeito == "vbc_ibscbs_divergente":
        tot["vBCIBSCBS"] += D("1.00")

    z = (lambda v: "0" if v == 0 else fmt(v)) if zero_curto else fmt
    total = sub(inf, "total")
    subs(sub(total, "ICMSTot"),
         [("vBC", fmt(tot["vBC"])), ("vICMS", fmt(tot["vICMS"])), ("vICMSDeson", fmt(tot["vICMSDeson"])),
          ("vFCP", "0.00"), ("vBCST", "0.00"), ("vST", "0.00"), ("vFCPST", "0.00"), ("vFCPSTRet", "0.00"),
          ("vProd", fmt(tot["vProd"])), ("vFrete", "0.00"), ("vSeg", "0.00"), ("vDesc", "0.00"), ("vII", "0.00"),
          ("vIPI", fmt(tot["vIPI"])), ("vIPIDevol", "0.00"), ("vPIS", fmt(tot["vPIS"])),
          ("vCOFINS", fmt(tot["vCOFINS"])), ("vOutro", "0.00"), ("vNF", fmt(vnf))])
    if args.com_is:
        sub(sub(total, "ISTot"), "vIS", fmt(tot["vIS"]))
    it = sub(total, "IBSCBSTot")
    sub(it, "vBCIBSCBS", fmt(tot["vBCIBSCBS"]))
    gibs = sub(it, "gIBS")
    for grp, suf, tag_v in (("gIBSUF", "UF", "vIBSUF"), ("gIBSMun", "Mun", "vIBSMun")):
        subs(sub(gibs, grp), [("vDif", z(tot[f"vDif{suf}"])), ("vDevTrib", z(tot[f"vDev{suf}"])), (tag_v, fmt(tot[tag_v]))])
    subs(gibs, [("vIBS", fmt(tot["vIBSUF"] + tot["vIBSMun"])), ("vCredPres", z(0)), ("vCredPresCondSus", z(0))])
    subs(sub(it, "gCBS"), [("vDif", z(tot["vDifCBS"])), ("vDevTrib", z(tot["vDevCBS"])), ("vCBS", fmt(tot["vCBS"])),
                           ("vCredPres", z(0)), ("vCredPresCondSus", z(0))])
    if rng.random() < 0.7:
        sub(total, "vNFTot", fmt(vnf))

    tr = sub(inf, "transp")
    mod_frete = rng.choice(["0", "1", "2", "9"])
    sub(tr, "modFrete", mod_frete)
    if mod_frete != "9":
        t = _pj(rng, rng.choice(list(UFS)))
        subs(sub(tr, "transporta"), [("CNPJ", t.doc), ("xNome", f"TRANSPORTES {rng.choice(SUFIXOS_PJ)} LTDA"),
                                     ("IE", t.ie), ("xEnder", f"{t.lgr}, {t.nro}"), ("xMun", t.xmun), ("UF", t.uf)])
        peso = fmt(rng.uniform(10, 30000), 3)
        subs(sub(tr, "vol"), [("qVol", rng.randint(1, 40)), ("esp", rng.choice(["GRANEL", "UND", "CX", "VOLUME"])),
                              ("pesoL", peso), ("pesoB", peso)])
    if tpnf == "1" and rng.random() < 0.7:
        cobr = sub(inf, "cobr")
        subs(sub(cobr, "fat"), [("nFat", f"{nnf}/1"), ("vOrig", fmt(vnf)), ("vDesc", "0.00"), ("vLiq", fmt(vnf))])
        n_dup = rng.choice([1, 1, 2, 3])
        parcelas = [q(vnf / n_dup)] * (n_dup - 1)
        parcelas.append(vnf - sum(parcelas))
        for k, vp in enumerate(parcelas, 1):
            subs(sub(cobr, "dup"), [("nDup", f"{k:03d}"), ("dVenc", f"{dt + timedelta(days=30 * k):%Y-%m-%d}"),
                                    ("vDup", fmt(vp))])
    dp = sub(sub(inf, "pag"), "detPag")
    if tipo == "devolucao":
        subs(dp, [("tPag", "90"), ("vPag", "0.00")])
    else:
        subs(dp, [("indPag", rng.choice(["0", "1"])), ("tPag", rng.choice(["01", "03", "15", "17"])), ("vPag", fmt(vnf))])
    cpl = "DOCUMENTO SINTETICO PARA TESTES - SEM VALOR FISCAL."
    if ref_chave:
        cpl += f" DEVOLUCAO REF. NF-E CHAVE {ref_chave}."
    sub(sub(inf, "infAdic"), "infCpl", cpl)
    subs(sub(inf, "infRespTec"), [("CNPJ", "11222333000181"), ("xContato", "RESPONSAVEL TECNICO TESTE"),
                                  ("email", "nfe@exemplo.invalid"), ("fone", "6230000000")])

    # assinatura com estrutura real e conteúdo aleatório (não verificável)
    b64 = lambda k: base64.b64encode(rng.randbytes(k)).decode()  # noqa: E731
    sig = sub(nfe, "Signature", ns=DS)
    si = sub(sig, "SignedInfo", ns=DS)
    sub(si, "CanonicalizationMethod", ns=DS, Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315")
    sub(si, "SignatureMethod", ns=DS, Algorithm=f"{DS}rsa-sha1")
    ref = sub(si, "Reference", ns=DS, URI=f"#NFe{chave}")
    trs = sub(ref, "Transforms", ns=DS)
    sub(trs, "Transform", ns=DS, Algorithm=f"{DS}enveloped-signature")
    sub(trs, "Transform", ns=DS, Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315")
    sub(ref, "DigestMethod", ns=DS, Algorithm=f"{DS}sha1")
    sub(ref, "DigestValue", b64(20), ns=DS)
    sub(sig, "SignatureValue", b64(256), ns=DS)
    sub(sub(sub(sig, "KeyInfo", ns=DS), "X509Data", ns=DS), "X509Certificate", b64(600), ns=DS)

    root = nfe
    if not args.sem_protocolo:  # nfeProc = NF-e + protocolo de autorização (formato de distribuição)
        root = ET.Element(f"{{{NS}}}nfeProc", versao="4.00")
        root.append(nfe)
        ip = sub(sub(root, "protNFe", versao="4.00"), "infProt")
        subs(ip, [("tpAmb", "2"), ("verAplic", "GO4.0"), ("chNFe", chave),
                  ("dhRecbto", (dt + timedelta(seconds=rng.randint(1, 30))).strftime("%Y-%m-%dT%H:%M:%S-03:00")),
                  ("nProt", f"1{cuf}{rng.randint(10**12, 10**13 - 1)}"), ("digVal", b64(20)),
                  ("cStat", "100"), ("xMotivo", "Autorizado o uso da NF-e")])
    return chave, root, nfe, linhas


def serializar(root) -> str:
    xml = ET.tostring(root, encoding="unicode")
    # Signature com xmlns default próprio (sem prefixo), como no XML real
    xml = (xml.replace(f' xmlns:ds="{DS}"', "").replace("<ds:Signature>", f'<Signature xmlns="{DS}">')
              .replace("<ds:", "<").replace("</ds:", "</"))
    return '<?xml version="1.0" encoding="UTF-8"?>' + xml


def escrever_atomico(destino: Path, conteudo: str) -> None:
    tmp = destino.with_suffix(".tmp")
    tmp.write_text(conteudo, encoding="utf-8")
    os.replace(tmp, destino)


# ------------------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inbox", required=True, type=Path, help="pasta de chegada monitorada pelo Airflow")
    ap.add_argument("--n", type=int, default=200, help="notas por lote")
    ap.add_argument("--lotes", type=int, default=1, help="quantos lotes gerar")
    ap.add_argument("--intervalo", type=float, default=0, help="segundos entre lotes (simula chegada contínua)")
    ap.add_argument("--data-ini", type=date.fromisoformat, help="dhEmi mínimo (padrão: últimas 24h)")
    ap.add_argument("--data-fim", type=date.fromisoformat, help="dhEmi máximo")
    ap.add_argument("--seed", type=int, help="seed das notas (padrão: aleatória)")
    ap.add_argument("--seed-cadastro", type=int, default=42, help="seed do cadastro (fixa = mesmas empresas)")
    ap.add_argument("--emitentes", type=int, default=300)
    ap.add_argument("--pct-cnpj-alfa", type=float, default=0.05, help="fração de CNPJ alfanumérico")
    ap.add_argument("--pct-mudanca-cadastral", type=float, default=0.10)
    ap.add_argument("--defect-rate", type=float, default=0.0)
    ap.add_argument("--dup-rate", type=float, default=0.0)
    ap.add_argument("--corrupt-rate", type=float, default=0.0)
    ap.add_argument("--com-is", action="store_true", help="inclui Imposto Seletivo (simulação 2027+)")
    ap.add_argument("--sem-protocolo", action="store_true", help="raiz <NFe> em vez de <nfeProc>")
    ap.add_argument("--xsd", type=Path, help="nfe_v4.00.xsd: valida cada NF-e gerada (requer lxml)")
    ap.add_argument("--manifesto", type=Path, help="CSV com as anomalias injetadas (gabarito)")
    a = ap.parse_args()

    a.inbox.mkdir(parents=True, exist_ok=True)
    rng = random.Random(a.seed)
    cad = Cadastro(a.seed_cadastro, a.emitentes, n_dest_pj=a.emitentes * 3, n_dest_pf=a.emitentes * 5,
                   pct_alfa=a.pct_cnpj_alfa, pct_mudanca=a.pct_mudanca_cadastral)
    schema = None
    if a.xsd:
        from lxml import etree  # opcional: só para validação
        schema = etree.XMLSchema(etree.parse(str(a.xsd)))
    ET.register_namespace("", NS)
    ET.register_namespace("ds", DS)

    manifesto = []
    for lote in range(1, a.lotes + 1):
        agora = datetime.now().replace(microsecond=0)
        ini = datetime.combine(a.data_ini, datetime.min.time()) if a.data_ini else agora - timedelta(days=1)
        fim = datetime.combine(a.data_fim, datetime.max.time().replace(microsecond=0)) if a.data_fim else agora
        janela = max(1, int((fim - ini).total_seconds()))
        cont = {"ok": 0, "defeito": 0, "dup": 0, "corrompido": 0}
        for _ in range(a.n):
            dt = ini + timedelta(seconds=rng.randint(0, janela))
            defeito = rng.choice(["tot_cbs_divergente", "vbc_ibscbs_divergente", "cclasstrib_incoerente"]) \
                if rng.random() < a.defect_rate else ""
            chave, root, nfe, _ = gerar_nota(rng, cad, dt, a, defeito)
            if schema is not None and not defeito:
                from lxml import etree
                if not schema.validate(etree.fromstring(serializar(nfe).encode())):
                    raise SystemExit(f"XSD inválido ({chave}): {schema.error_log.last_error}")
            xml = serializar(root)
            sufixo = "procNFe" if not a.sem_protocolo else "nfe"
            arquivo = a.inbox / f"{chave}-{sufixo}.xml"
            if rng.random() < a.corrupt_rate:
                xml = xml[: rng.randint(200, len(xml) // 2)]           # truncado no transporte
                manifesto.append((lote, arquivo.name, chave, "xml_corrompido"))
                cont["corrompido"] += 1
            escrever_atomico(arquivo, xml)
            if defeito:
                manifesto.append((lote, arquivo.name, chave, defeito))
                cont["defeito"] += 1
            if rng.random() < a.dup_rate:                               # reenvio do mesmo documento
                dup = a.inbox / f"{chave}-{sufixo}-reenvio{rng.randint(1, 9)}.xml"
                escrever_atomico(dup, xml)
                manifesto.append((lote, dup.name, chave, "duplicada"))
                cont["dup"] += 1
            cont["ok"] += 1
        print(f"lote {lote}/{a.lotes}: {cont['ok']} notas -> {a.inbox} "
              f"(defeitos={cont['defeito']} dup={cont['dup']} corrompidos={cont['corrompido']})", flush=True)
        if lote < a.lotes and a.intervalo:
            time.sleep(a.intervalo)

    if a.manifesto and manifesto:
        novo = not a.manifesto.exists()
        with open(a.manifesto, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if novo:
                w.writerow(["lote", "arquivo", "chave", "anomalia"])
            w.writerows(manifesto)


if __name__ == "__main__":
    main()
