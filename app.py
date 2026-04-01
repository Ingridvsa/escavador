from __future__ import annotations

import json
import pandas as pd
import streamlit as st

from db import (
    init_db,
    listar_callbacks,
    listar_movimentacoes_processo,
    listar_processos,
    listar_todas_movimentacoes,
)
from escavador_api import (
    EscavadorAPIError,
    criar_monitoramento,
    detalhes_origem,
    listar_monitoramentos,
    registrar_consulta_inicial,
)

st.set_page_config(page_title="Monitoramento Escavador", layout="wide")
init_db()

st.title("Monitoramento de Processos - Escavador")
st.caption("SQLite + Streamlit + webhook")

tab1, tab2, tab3, tab4 = st.tabs([
    "Cadastrar processo",
    "Processos",
    "Movimentações",
    "Callbacks",
])

with tab1:
    st.subheader("Novo processo")

    with st.form("cadastro_processo"):
        numero_cnj = st.text_input("Número CNJ", value="0000074-20.2026.5.06.0012")
        origem = st.text_input("Origem", value="TRT-6")
        frequencia = st.selectbox("Frequência", ["DIARIA", "SEMANAL"], index=0)
        fazer_consulta = st.checkbox("Fazer consulta inicial agora", value=True)
        criar_monitor = st.checkbox("Criar monitoramento agora", value=True)
        submitted = st.form_submit_button("Cadastrar")

    if submitted:
        try:
            info_origem = detalhes_origem(origem)
            st.success(f"Origem válida: {info_origem.get('nome')}")

            if fazer_consulta:
                with st.spinner("Consultando processo..."):
                    resultado = registrar_consulta_inicial(numero_cnj, origem)
                st.write("### Resultado da consulta inicial")
                st.json(resultado)

            if criar_monitor:
                with st.spinner("Criando monitoramento..."):
                    monitor = criar_monitoramento(numero_cnj, origem, frequencia)
                st.write("### Monitoramento criado")
                st.json(monitor)

            st.success("Processo cadastrado com sucesso.")

        except EscavadorAPIError as e:
            st.error(str(e))
        except Exception as e:
            st.exception(e)

with tab2:
    st.subheader("Processos cadastrados")
    rows = listar_processos()
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum processo cadastrado.")

    if st.button("Atualizar lista de monitoramentos do Escavador"):
        try:
            result = listar_monitoramentos()
            st.json(result)
        except Exception as e:
            st.error(str(e))

with tab3:
    st.subheader("Movimentações")

    rows_proc = listar_processos()
    numeros = [r["numero_cnj"] for r in rows_proc]

    numero_sel = st.selectbox(
        "Filtrar por processo",
        options=["Todos"] + numeros if numeros else ["Todos"]
    )

    if numero_sel != "Todos":
        movs = listar_movimentacoes_processo(numero_sel)
    else:
        movs = listar_todas_movimentacoes()

    if movs:
        df_mov = pd.DataFrame([dict(r) for r in movs])
        st.dataframe(df_mov, use_container_width=True, height=500)
    else:
        st.info("Nenhuma movimentação salva.")

with tab4:
    st.subheader("Callbacks recebidos")
    callbacks = listar_callbacks()

    if callbacks:
        df_cb = pd.DataFrame([dict(r) for r in callbacks])
        st.dataframe(df_cb, use_container_width=True, height=350)

        with st.expander("Ver último payload"):
            st.code(callbacks[0]["payload_json"], language="json")
    else:
        st.info("Nenhum callback recebido ainda.")