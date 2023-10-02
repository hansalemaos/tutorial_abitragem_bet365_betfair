# pip install numpy flatten_everything normaltext seleniumbase a_selenium2df PrettyColorPrinter a_pandas_ex_bs4df lxml rapidfuzz fuzzypandaswuzzy a_pandas_ex_apply_ignore_exceptions 
import re
from collections import defaultdict
from time import sleep
import numpy as np
from flatten_everything import flatten_everything
from normaltext import lookup
from selenium.webdriver.support import expected_conditions
from seleniumbase import Driver
from selenium.webdriver.support.wait import WebDriverWait
from a_selenium2df import get_df
from selenium.webdriver.common.by import By
from PrettyColorPrinter import add_printer
import threading
from a_pandas_ex_bs4df import pd_add_bs4_to_df

import lxml
from rapidfuzz import fuzz
from fuzzypandaswuzzy import pd_add_fuzzy_all
from a_pandas_ex_apply_ignore_exceptions import pd_add_apply_ignore_exceptions
import pandas as pd

pd_add_fuzzy_all()
add_printer(1)
drivers = {}
pd_add_apply_ignore_exceptions()
pd_add_bs4_to_df()
tempo_de_dormir_depois_abrir_o_site = 5


def formatar_df(dfresultado2):
    for col in dfresultado2.columns:
        ba = "".join([str(r) for r in dfresultado2[col].astype("string").to_list()])
        if "." in ba:
            try:
                dfresultado2[col] = dfresultado2[col].astype("Float64")
                continue
            except Exception:
                pass
        else:
            try:
                dfresultado2[col] = dfresultado2[col].astype("Int64")
                continue
            except Exception:
                pass
        dfresultado2[col] = dfresultado2[col].astype("string")
    return dfresultado2


def abrir_site(nome, q, with_methods):
    drivers[nome].get(nome)
    sleep(tempo_de_dormir_depois_abrir_o_site)

    df = pd.DataFrame()
    while df.empty:
        df = obter_dataframe(drivers[nome], q, with_methods)
    resultados[nome] = df


def obter_dataframe(driver, query="*", with_methods=True):
    df = pd.DataFrame()
    while df.empty:
        df = get_df(
            driver,
            By,
            WebDriverWait,
            expected_conditions,
            queryselector=query,
            with_methods=with_methods,
        )
    return df


def normalizar(sen):
    norm = "".join(
        [
            lookup(k, case_sens=True, replace="x", add_to_printable="")["suggested"]
            for k in sen
        ]
    )
    return re.sub(r"\W+", "", norm)


nomes = [
    "https://www.betfair.com/exchange/plus/pt/futebol/brasileiro-s%C3%A9rie-a-apostas-13",
    "https://www.bet365.com/#/AC/B1/C1/D1002/E88369731/G40/",
]
for nome in nomes:
    drivers[nome] = Driver(uc=True)

resultados = {}

for nome in nomes:
    try:
        threading.Thread(target=abrir_site, args=(nome, "*", True)).start()
    except Exception as e:
        print(e)
        continue
while len(resultados) < len(nomes):
    sleep(1)
df = resultados[nomes[1]].copy()
df.loc[
    df.aa_classList.str.contains("ccm-CookieConsentPopup_Accept", regex=False, na=False)
].se_click.iloc[0]()
sleep(2)
df = obter_dataframe(drivers[nomes[1]], query="*")

df3 = df.dropna(subset=["aa_scrollHeight", "aa_offsetLeft"]).sort_values(
    by=[
        "aa_scrollHeight",
        "aa_offsetLeft",
    ],
)
df4 = df3.loc[
    df3.aa_classList.str.contains("gl-Market_General-cn1", regex=False, na=False)
]
# odds = df4.sort_values(by="aa_offsetLeft") -> ERRADO
didi = defaultdict(list)
for name, group in df4.groupby("aa_offsetHeight"):
    didi[name] = group.copy()
odds = didi[max(didi)].sort_values(by="aa_offsetLeft")
todososteams = df3.loc[
    df3.aa_classList.str.contains(
        "rcl-ParticipantFixtureDetails_LhsContainerInner", na=False
    )
]
timeeodds = np.array_split(odds, len(odds) // len(todososteams))
timeeodds = [
    h.sort_values(by="aa_offsetTop")
    for h in sorted(timeeodds, key=lambda x: x.aa_offsetLeft.iloc[0])
]
teamsarrumado = todososteams.aa_innerText.str.strip().str.split("\n", expand=True)
teamssplit = (
    teamsarrumado[[0, 1, 2]]
    .reset_index(drop=True)
    .rename(columns={0: "aa_comeco", 1: "aa_nome1", 2: "aa_nome_2"})
    .copy()
)

dfresultado2 = pd.concat(
    [
        teamssplit,
        pd.concat(
            [q.reset_index(drop=True).aa_innerText for q in timeeodds],
            axis=1,
            ignore_index=True,
        ).rename(
            columns={0: "aa_time1", 1: "aa_empate", 2: "aa_time2", 3: "aa_sei_la"}
        ),
    ],
    axis=1,
)

dfresultado2 = formatar_df(dfresultado2)
df = obter_dataframe(drivers[nomes[0]], query="*")
df3 = df.dropna(subset=["aa_scrollHeight", "aa_offsetLeft"]).sort_values(
    by=[
        "aa_scrollHeight",
        "aa_offsetLeft",
    ],
    ascending=[True, True],
)
dfo = df3.loc[df3.aa_localName == "td"]

dfsoup = dfo.aa_innerHTML.ds_apply_ignore(pd.NA, lambda x: pd.Q_bs4_to_df(x))
alldata = []
for dfs in dfsoup:
    bax = dfs.loc[dfs.aa_name == "label"]
    baxflat = list(flatten_everything([q if q else pd.NA for q in bax.aa_contents]))
    print(baxflat)
    if "R$" not in "".join([str(q) for q in baxflat]):
        continue
    alldata.append(baxflat)

dfresultado3 = pd.concat(
    [
        pd.DataFrame(
            np.reshape(
                df3.loc[
                    (df3.aa_tagName == "LI") & (df3.aa_className == "name")
                ].aa_innerText,
                (-1, 2),
            ),
            columns=["bb_nome1", "bb_nome2"],
        ),
        pd.DataFrame(alldata),
    ],
    axis=1,
)

hora = (
    (
        df.loc[
            df.aa_classList.str.contains(r"scrollable-panes-height-taker", na=False)
        ].sort_values(by="aa_offsetWidth")
    )
    .aa_innerText.iloc[0]
    .splitlines()
)

dfhora = pd.DataFrame([x for x in hora if " x " in x or re.findall(r"\s+\d+\s+", x)])

sohora = dfhora.loc[dfhora[0].str.contains(r"\s+\d+\s+")]
dfhora["bb_hora"] = sohora

dfhora.bb_hora = dfhora.bb_hora.ffill()
dfhora = dfhora.loc[np.setdiff1d(dfhora.index, sohora.index)].reset_index(drop=True)
dfdata = pd.concat(
    [
        dfhora.bb_hora,
        dfhora[0]
        .str.split(" x ", expand=True)
        .rename(columns={0: "cc_nome1", 1: "cc_nome2"}),
    ],
    axis=1,
)
dfresultado3 = formatar_df(dfresultado3.replace(r"^\s*R\$", "", regex=True))
dfresultado3.columns = [
    "bb_" + str(x) if isinstance(x, int) else x for x in dfresultado3.columns
]
dfresultado2.apply(
    lambda x: normalizar(str(x.aa_nome1).lower() + str(x.aa_nome_2).lower()), axis=1
)

dfresultado2["xx_normalizado2"] = dfresultado2.apply(
    lambda x: normalizar(str(x.aa_nome1).lower() + str(x.aa_nome_2).lower()), axis=1
)
dfresultado3["xx_normalizado3"] = dfresultado3.apply(
    lambda x: normalizar(str(x.bb_nome1).lower() + str(x.bb_nome2).lower()), axis=1
)
dfdata["xx_normalizado4"] = dfdata.apply(
    lambda x: normalizar(str(x.cc_nome1).lower() + str(x.cc_nome2).lower()), axis=1
)
c2 = (
    pd.concat(
        [dfresultado2["xx_normalizado2"], dfresultado3["xx_normalizado3"]], axis=1
    )
    .d_fuzzy2cols(scorer=fuzz.QRatio)
    .sort_values(by="aa_match")
    .reset_index(drop=True)
)
a1 = c2.loc[c2.aa_value1.isin(dfresultado2.xx_normalizado2)]
a2 = c2.loc[c2.aa_value2.isin(dfresultado3.xx_normalizado3)]
lookupdict = {}
for key, item in a1.iterrows():
    lookupdict[item.aa_value1] = item.aa_value1

for key, item in a2.iterrows():
    if item.aa_value2 not in lookupdict:
        lookupdict[item.aa_value2] = item.aa_value1

dfresultado3["mergecolumn"] = dfresultado3.xx_normalizado3.map(lookupdict)
dfresultado2["mergecolumn"] = dfresultado2.xx_normalizado2.map(lookupdict)
dfdata["mergecolumn"] = dfdata.xx_normalizado4.map(lookupdict)
resultadosfinal = pd.merge(
    pd.merge(dfresultado2, dfresultado3, right_on="mergecolumn", left_on="mergecolumn"),
    dfdata,
    right_on="mergecolumn",
    left_on="mergecolumn",
)
resultadosfinal.columns = [
    "bet365_" + x[3:]
    if x.startswith("aa_")
    else x
    if not x.startswith("bb_")
    else "betfair_" + x[3:]
    for x in resultadosfinal.columns
]
resultadosfinal = resultadosfinal.drop(
    columns=[
        x
        for x in [
            "xx_normalizado2",
            "mergecolumn",
            "xx_normalizado3",
            "cc_nome1",
            "cc_nome2",
            "xx_normalizado4",
        ]
        if x in resultadosfinal.columns
    ]
)
