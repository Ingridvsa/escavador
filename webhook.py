from __future__ import annotations

import hashlib
import json

from fastapi import FastAPI, Request

from db import (
    init_db,
    inserir_movimentacao,
    salvar_callback,
    upsert_processo,
)

app = FastAPI(title="Webhook Escavador")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/callback/escavador")
async def callback_escavador(request: Request):
    payload = await request.json()

    evento = payload.get("event") or payload.get("evento")
    status_callback = payload.get("status")
    item_id_externo = str(payload.get("id") or payload.get("uuid") or "")

    salvar_callback(
        evento=evento,
        item_id_externo=item_id_externo,
        status_callback=status_callback,
        payload_json=json.dumps(payload, ensure_ascii=False),
    )

    # callback de monitoramento de diário
    if evento == "diario_movimentacao_nova":
        monitoramento = (payload.get("monitoramento") or [{}])[0]
        processo = monitoramento.get("processo") or {}
        numero_cnj = processo.get("numero_novo") or processo.get("numero_antigo")
        origem = monitoramento.get("tribunal_sigla") or processo.get("origem_tribunal_id") or "N/A"

        if numero_cnj:
            processo_id = upsert_processo(numero_cnj, str(origem))
            mov = payload.get("movimentacao") or {}

            data_mov = mov.get("data") or mov.get("data_formatada")
            conteudo = mov.get("snippet") or mov.get("conteudo") or mov.get("descricao_pequena") or "Nova movimentação em diário"
            hash_unico = hashlib.sha256(
                f"{processo_id}|{data_mov}|{conteudo}".encode("utf-8")
            ).hexdigest()

            inserir_movimentacao(
                processo_id=processo_id,
                data_movimentacao=data_mov,
                conteudo=conteudo,
                instancia=None,
                orgao_julgador=mov.get("secao"),
                classe=mov.get("tipo"),
                assunto=mov.get("subtipo"),
                origem_evento=evento,
                hash_unico=hash_unico,
                payload_json=json.dumps(payload, ensure_ascii=False),
            )

    # callback típico de resultado async / movimentação nova em processo
    processo = payload.get("processo") or {}
    event_data = payload.get("event_data") or {}

    numero_cnj = processo.get("numero_unico") or processo.get("numero_novo")
    origem = processo.get("origem")

    if numero_cnj and evento in {"movimentacao_nova", "resultado_processo_async"}:
        processo_id = upsert_processo(numero_cnj, origem or "N/A")

        # alguns callbacks trazem a movimentação em event_data
        data_mov = event_data.get("data")
        conteudo = event_data.get("conteudo") or event_data.get("descricao")
        if conteudo:
            hash_unico = hashlib.sha256(
                f"{processo_id}|{data_mov}|{conteudo}".encode("utf-8")
            ).hexdigest()

            inserir_movimentacao(
                processo_id=processo_id,
                data_movimentacao=data_mov,
                conteudo=conteudo,
                instancia=processo.get("instancia"),
                orgao_julgador=processo.get("orgao_julgador"),
                classe=processo.get("classe"),
                assunto=processo.get("assunto"),
                origem_evento=evento,
                hash_unico=hash_unico,
                payload_json=json.dumps(payload, ensure_ascii=False),
            )

    return {"status": "ok"}