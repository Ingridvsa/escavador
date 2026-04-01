from __future__ import annotations

import os
import pandas as pd
import streamlit as st

from db import init_db, listar_callbacks, listar_movimentacoes, listar_processos
from escavador_service import (
    EscavadorErro,
    criar_monitoramento_tribunal,
    registrar_consulta_inicial,
)

st.set_page_config(page_title="Painel de Processos", layout="wide")
init_db()

st.title("Painel de Atualização de Processos")
st.caption("Render + Streamlit + Postgres")

webhook_url = os.getenv("WEBHOOK_PUBLIC_URL", "")

col1, col2 = st.columns([2, 1])
with col1:
    st.write("Use este painel para cadastrar o processo, fazer a consulta inicial e ativar o monitoramento.")
with col2:
    if webhook_url:
        st.info(f"Webhook: {webhook_url}")

aba1, aba2, aba3 = st.tabs(["Cadastrar processo", "Movimentações", "Callbacks"])

with aba1:
    st.subheader("Cadastrar e monitorar processo")

    with st.form("form_processo"):
        numero_cnj = st.text_input("Número do processo", value="")
        tribunal = st.text_input("Tribunal", value="TRT-6")
        frequencia = st.selectbox("Frequência", ["DIARIA", "SEMANAL"], index=0)
        fazer_consulta = st.checkbox("Fazer consulta inicial", value=True)
        criar_monitoramento = st.checkbox("Criar monitoramento no tribunal", value=True)
        enviar = st.form_submit_button("Salvar")

    if enviar:
        try:
            if not numero_cnj.strip():
                st.error("Informe o número do processo.")
            else:
                if fazer_consulta:
                    with st.spinner("Consultando processo no tribunal..."):
                        resultado = registrar_consulta_inicial(numero_cnj, tribunal)
                    st.success("Consulta inicial concluída")
                    st.json(resultado)

                if criar_monitoramento:
                    with st.spinner("Criando monitoramento..."):
                        monitor = criar_monitoramento_tribunal(numero_cnj, tribunal, frequencia)
                    st.success("Monitoramento criado")
                    st.json(monitor)

        except EscavadorErro as e:
            st.error(str(e))
        except Exception as e:
            st.exception(e)

    st.divider()
    st.subheader("Processos cadastrados")
    processos = listar_processos()
    if processos:
        df = pd.DataFrame(processos)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum processo cadastrado ainda.")

with aba2:
    st.subheader("Movimentações")
    processos = listar_processos()
    opcoes = ["Todos"] + [p["numero_cnj"] for p in processos] if processos else ["Todos"]
    filtro = st.selectbox("Filtrar por processo", opcoes)

    rows = listar_movimentacoes(None if filtro == "Todos" else filtro)
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=500)
    else:
        st.info("Nenhuma movimentação registrada.")

with aba3:
    st.subheader("Callbacks recebidos")
    rows = listar_callbacks()
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=350)
    else:
        st.info("Nenhum callback recebido.")