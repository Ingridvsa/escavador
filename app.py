from __future__ import annotations

import json
import os
import pandas as pd
import streamlit as st

from db import (
    buscar_processo_por_numero,
    init_db,
    listar_callbacks,
    listar_movimentacoes,
    listar_processos,
    processo_existe,
)
from escavador_service import (
    EscavadorErro,
    cadastrar_processo_se_nao_existir,
    criar_monitoramento_tribunal,
)

st.set_page_config(page_title="Painel de Processos", layout="wide")
init_db()

st.title("Painel de Processos")
st.caption("Consulta única + classificação + monitoramento")

webhook_url = os.getenv("WEBHOOK_PUBLIC_URL", "")
if webhook_url:
    st.info(f"Webhook configurado: {webhook_url}")

abas = st.tabs(["Cadastrar", "Processos", "Movimentações", "Callbacks"])

with abas[0]:
    st.subheader("Cadastrar novo processo")

    with st.form("form_cadastro"):
        numero_cnj = st.text_input("Número do processo")
        tribunal = st.text_input("Tribunal", value="TRT-6")
        frequencia = st.selectbox("Frequência do monitoramento", ["DIARIA", "SEMANAL"], index=0)
        enviar = st.form_submit_button("Cadastrar processo")

    if enviar:
        try:
            numero_cnj = numero_cnj.strip()
            tribunal = tribunal.strip().upper()

            if not numero_cnj:
                st.error("Informe o número do processo.")
            elif processo_existe(numero_cnj):
                st.warning("Esse processo já está cadastrado na sua base.")
            else:
                with st.spinner("Consultando processo..."):
                    resultado = cadastrar_processo_se_nao_existir(numero_cnj, tribunal)

                with st.spinner("Criando monitoramento..."):
                    monitor = criar_monitoramento_tribunal(numero_cnj, tribunal, frequencia)

                st.success("Processo cadastrado e monitoramento criado com sucesso.")

                with st.expander("Resultado da consulta"):
                    st.json(resultado)

                with st.expander("Resultado do monitoramento"):
                    st.json(monitor)

        except EscavadorErro as e:
            st.error(str(e))
        except Exception as e:
            st.exception(e)

with abas[1]:
    st.subheader("Processos cadastrados")

    processos = listar_processos()

    if not processos:
        st.info("Nenhum processo cadastrado.")
    else:
        grupos: dict[str, list[dict]] = {}

        for proc in processos:
            tipo = proc.get("tipo_processo") or "Sem classificação"
            subtipo = proc.get("subtipo_processo")
            chave = tipo if not subtipo else f"{tipo} - {subtipo}"
            grupos.setdefault(chave, []).append(proc)

        for grupo, itens in grupos.items():
            with st.expander(f"{grupo} ({len(itens)})", expanded=True):
                for proc in itens:
                    label = f"{proc['numero_cnj']} — {proc.get('nome_parte_principal') or 'Sem parte principal'}"
                    if st.button(label, key=f"btn_{proc['numero_cnj']}"):
                        st.session_state["processo_selecionado"] = proc["numero_cnj"]

        processo_selecionado = st.session_state.get("processo_selecionado")
        if processo_selecionado:
            st.divider()
            st.subheader("Detalhes do processo")

            proc = buscar_processo_por_numero(processo_selecionado)
            if proc:
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Número:** {proc.get('numero_cnj')}")
                    st.write(f"**Tribunal:** {proc.get('tribunal')}")
                    st.write(f"**Tipo:** {proc.get('tipo_processo')}")
                    st.write(f"**Subtipo:** {proc.get('subtipo_processo') or '-'}")
                    st.write(f"**Parte principal:** {proc.get('nome_parte_principal') or '-'}")
                with col2:
                    st.write(f"**Valor da causa:** {proc.get('valor_causa') or '-'}")
                    st.write(f"**Data de distribuição:** {proc.get('data_distribuicao') or '-'}")
                    st.write(f"**Monitoramento ID:** {proc.get('monitoramento_id') or '-'}")
                    st.write(f"**Frequência:** {proc.get('frequencia') or '-'}")
                    st.write(f"**Status monitoramento:** {proc.get('status_monitoramento') or '-'}")

                st.markdown("### Movimentações")
                movs = listar_movimentacoes(processo_selecionado)
                if movs:
                    df_mov = pd.DataFrame(movs)
                    colunas = [
                        c for c in [
                            "data_movimentacao",
                            "conteudo",
                            "instancia",
                            "orgao_julgador",
                            "classe",
                            "assunto",
                            "origem_evento",
                            "criado_em",
                        ] if c in df_mov.columns
                    ]
                    st.dataframe(df_mov[colunas], use_container_width=True, height=400)
                else:
                    st.info("Nenhuma movimentação encontrada.")

                if proc.get("payload_consulta_json"):
                    with st.expander("Payload completo da consulta inicial"):
                        try:
                            st.json(json.loads(proc["payload_consulta_json"]))
                        except Exception:
                            st.code(proc["payload_consulta_json"], language="json")

with abas[2]:
    st.subheader("Todas as movimentações")
    movs = listar_movimentacoes()
    if movs:
        df = pd.DataFrame(movs)
        st.dataframe(df, use_container_width=True, height=500)
    else:
        st.info("Nenhuma movimentação registrada.")

with abas[3]:
    st.subheader("Callbacks recebidos")
    callbacks = listar_callbacks()
    if callbacks:
        df = pd.DataFrame(callbacks)
        st.dataframe(df, use_container_width=True, height=350)

        with st.expander("Último payload recebido"):
            st.code(callbacks[0]["payload_json"], language="json")
    else:
        st.info("Nenhum callback recebido.")