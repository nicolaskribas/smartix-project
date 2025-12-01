import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    import marimo as mo
    import json
    return json, mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Versões do dicionário de Liu

    O dicionario foi introduzido no primeiro commit (`746276d`) e alterado uma única vez no commit `99f1bfa`, adicionando algumas comunidades. A diferença entre eles é a seguinte:

    ```diff
    --- 746276d_semanticdic_total.json	2025-11-29 16:59:44.894565623 -0300
    +++ 99f1bfa_semanticdic_total.json	2025-11-29 16:59:57.097563599 -0300
    @@ -303,7 +303,75 @@
               "Argentina"
             ]
           ]
    -    }
    +    },
    +    "pref": [
    +      [
    +        "explicit",
    +        10,
    +        10
    +      ],
    +      [
    +        "explicit",
    +        70,
    +        70
    +      ],
    +      [
    +        "explicit",
    +        120,
    +        120
    +      ],
    +      [
    +        "explicit",
    +        125,
    +        125
    +      ],
    +      [
    +        "explicit",
    +        130,
    +        130
    +      ],
    +      [
    +        "explicit",
    +        135,
    +        135
    +      ],
    +      [
    +        "explicit",
    +        140,
    +        140
    +      ]
    +    ],
    +    "sel_ann": {
    +      "no-export": [
    +        [
    +          "explicit",
    +          990,
    +          "BGP customers or peers"
    +        ],
    +        [
    +          "explicit",
    +          991,
    +          "peers"
    +        ]
    +      ]
    +    },
    +    "prepend": [
    +      [
    +        "explicit",
    +        3001,
    +        1
    +      ],
    +      [
    +        "explicit",
    +        3002,
    +        2
    +      ],
    +      [
    +        "explicit",
    +        3003,
    +        3
    +      ]
    +    ]
       },
       "209": {
         "pref": [
    ```
    """)
    return


@app.cell
def _():
    json_file_path = "liu2024collecting-dataset/results/dictionary/semanticdic_total.json"
    return (json_file_path,)


@app.cell
def _(pd):
    def load_community_dictionary(json):
        records = []

        for asn, type_subtype_content in json.items():
            for semmantic_type, subtype_content in type_subtype_content.items():
                conforming = True

                match semmantic_type:
                    case "tag" | "sel_ann":
                        # in this case 'subtype_content' is a dictionary with
                        # subtypes as keys and values contains a list of list of items
                        pass
                    case "blackhole" | "pref" | "prepend":
                        # in this cast 'subtype_content' is just a list of list of items
                        # so we set sub type to the type
                        subtype_content = {None: subtype_content}
                    case "loc" | "IXP":
                        # AS 37271 has loc and IXP, which should be under a tag, but are the
                        # second nesting level
                        conforming = False

                        # Fix: we wrap it in a dictionary with the correspondent sub type and set type
                        # to "tag"
                        subtype_content = {
                            semmantic_type: subtype_content
                        }  # ("loc" | "IXP): subtype content"
                        semmantic_type = "tag"
                    case t:
                        raise Exception(f"unexpected type {t}")

                for semmantic_sub_type, content in subtype_content.items():
                    if semmantic_type == "tag":
                        match semmantic_sub_type:
                            case "rel" | "loc" | "IXP" | "fac" | "asn":
                                pass
                            case "origin":
                                # ASes 8455 and 37721 has origin as sub type for tag
                                # Fix: do nothing
                                conforming = False
                            case s:
                                raise Exception(f"unexpected semmanting sub type {s}")

                    if isinstance(content[0][0], list):
                        # AS 12389 has pref that nests in a list
                        conforming = False

                        # Fix: we make sure that the list with content is the only element in the outer list
                        # then we use only this list
                        assert len(content) == 1
                        content = content[0]

                    for item in content:
                        if semmantic_type == "blackhole":
                            item.append(None)  # c is None

                        # a, b, c
                        community_value_type, community_value, semmantic_text = item

                        match community_value_type:
                            case "explicit":
                                community_value_numeric = community_value
                                community_value_pattern = None
                            case "regular":
                                community_value_numeric = None
                                community_value_pattern = community_value
                            case t:
                                raise Exception(f"unexpected community value type: {t}")

                        if semmantic_type == "tag" and semmantic_sub_type == "rel":
                            match semmantic_text:
                                case (
                                    "provider"
                                    | "peer"
                                    | "customer"
                                    | "partial customer"
                                    | "partial provider"
                                ):
                                    pass
                                case (
                                    "origin"
                                    | 30
                                    | 20
                                    | 10
                                    | "customer,peer"
                                    | "2-CA,3-Montreal"
                                    | "2-CA,3-Toronto"
                                    | "2-CA,3-Quebec"
                                    | "non-customer"
                                ):
                                    # ASes 12306, 12348, 21341, 22652, 37271
                                    # has this values, which are not documented
                                    conforming = False
                                    # Fix: we just mark as non-conforming
                                case m:
                                    raise Exception(f"unexpected meaning {m}")

                        records.append(
                            {
                                "AS Number": asn,
                                "Community Value Type": community_value_type,
                                "Community Value Numeric": community_value_numeric,
                                "Community Value Pattern": community_value_pattern,
                                "Semmantic Type": semmantic_type,
                                "Semmantic Sub Type": semmantic_sub_type,
                                "Semmantic Text": semmantic_text,
                                "Conforming": conforming,
                            }
                        )

        schema = {
            "AS Number": "string",
            "Community Value Type": "category",
            "Community Value Numeric": "UInt64",
            "Community Value Pattern": "string",
            "Semmantic Type": "category",
            "Semmantic Sub Type": "category",
            "Semmantic Text": "object",
            "Conforming": "bool",
        }
        return pd.DataFrame(records).astype(schema)
    return (load_community_dictionary,)


@app.cell
def _(json, json_file_path, load_community_dictionary):
    with open(json_file_path) as json_file:
        json_dict = json.load(json_file)

    df = load_community_dictionary(json_dict)
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Anomalias

    O formato do dicionário `json` é documentado pelo autor no arquivo `results/dictionary/README.md`.
    Mas encontrei registros fora do formato:

    - O AS 37271 possui comunidades de 'loc' e 'IXP' no mesmo nível de uma 'tag', quando na verdade deviam estar dentro de uma.
    - A lista de comunidades de _local preference_  do AS 12389 estão aninhados dentro de uma outra lista.
    - Os ASes 12306, 12348, 21341, 22652, 37271 possuem valores de 'rel' que não estão documentados: "origin", 30, 20, 10, "customer,peer", "2-CA,3-Montreal", "2-CA,3-Toronto", "2-CA,3-Quebec" e "non-customer".

    Consegui contornar todas as anomalias durante o _parsing_, aproveitando assim todos os dados. Adicionei uma nova columa ("Conforming") para identificar os registros fora do padrão.
    """)
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _():
    with open(
        "liu2024collecting-dataset/results/dictionary/community_websites.txt"
    ) as websites_list_file:
        ases = [line.split()[0] for line in websites_list_file]

    len(ases)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Semanticas

    Há comunidades em que se devide um valor para cada possível entidade, por exemplo, nas comunidades informativas de origem do anúncio deve haver um valor para cada possivel origem. Sendo assim, há uma grande quantidade de diferentes comunidades que tem a mesma semantica, nesse caso a de identificar a origem do anúncio.
    Nesse passo vou tentar determinar o que são diferentes semantica.
    """)
    return


@app.cell
def _(df):
    df.groupby(["Semmantic Type", "Semmantic Sub Type"], observed=True)["Semmantic Text"].nunique()
    return


@app.cell
def _(df):
    df[["Semmantic Type", "Semmantic Sub Type"]].drop_duplicates()
    return


@app.cell
def _(df):
    df_copy = df.copy()
    criteria = (
        (df_copy["Semmantic Type"] == "pref")
        | (df_copy["Semmantic Type"] == "prepend")
        | (df_copy["Semmantic Type"] == "sel_ann")
        | (df_copy["Semmantic Sub Type"] == "loc")
    )
    df_copy.loc[criteria, "Semmantic Text"] = None
    df_copy[["Semmantic Type", "Semmantic Sub Type", "Semmantic Text"]].drop_duplicates()
    return


if __name__ == "__main__":
    app.run()
