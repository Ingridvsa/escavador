from __future__ import annotations

import os
import hashlib
import json

from fastapi import FastAPI, Request, HTTPException

from db import (
    init_db,
    inserir_movimentacao,
    salvar_callback,
    upsert_processo_basico,
)

app = FastAPI(title="Webhook Escavador")

ESCAVADOR_CALLBACK_TOKEN = os.getenv("ESCAVADOR_CALLBACK_TOKEN")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/callback/escavador")
async def callback_escavador(request: Request):
    if ESCAVADOR_CALLBACK_TOKEN:
        auth_header = request.headers.get("Authorization", "")
        expected = f"Bearer {ESCAVADOR_CALLBACK_TOKEN}"

        if auth_header != expected:
            raise HTTPException(status_code=401, detail="Callback não autorizado")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Payload inválido")

    evento = payload.get("event") or payload.get("evento")
    item_id = str(payload.get("id") or payload.get("uuid") or "")

    salvar_callback(
        evento=evento,
        item_id_externo=item_id,
        payload_json=json.dumps(payload, ensure_ascii=False),
    )

    processo = payload.get("processo") or {}
    event_data = payload.get("event_data") or {}

    numero_cnj = processo.get("numero_unico") or processo.get("numero_novo")
    tribunal = processo.get("origem") or "N/A"

    if numero_cnj:
        processo_id = upsert_processo_basico(numero_cnj, tribunal)

        data_mov = event_data.get("data")
        conteudo = event_data.get("conteudo") or event_data.get("descricao")

        if conteudo:
            chave = hashlib.sha256(
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
                chave_unica=chave,
                payload_json=json.dumps(payload, ensure_ascii=False),
            )

    return {"status": "ok"}