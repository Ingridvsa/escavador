from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

from db import (
    atualizar_monitoramento,
    inserir_movimentacao,
    salvar_consulta,
    upsert_processo,
)

load_dotenv()

API_TOKEN = os.getenv("ESCAVADOR_API_TOKEN")
BASE_URL = os.getenv("ESCAVADOR_BASE_URL", "https://api.escavador.com/api/v1")
TIMEOUT = 30


class EscavadorErro(Exception):
    pass


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    }


def _request(method: str, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not API_TOKEN:
        raise EscavadorErro("ESCAVADOR_API_TOKEN não encontrado.")

    url = f"{BASE_URL}{endpoint}"

    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=_headers(),
            json=payload,
            timeout=TIMEOUT,
        )
    except requests.RequestException as e:
        raise EscavadorErro(f"Erro de conexão: {e}") from e

    try:
        data = resp.json()
    except ValueError:
        raise EscavadorErro(f"Resposta inválida: {resp.text[:500]}")

    if resp.status_code >= 400:
        raise EscavadorErro(f"Erro HTTP {resp.status_code}: {json.dumps(data, ensure_ascii=False)}")

    return data


def consultar_processo(numero_cnj: str, tribunal: str) -> dict[str, Any]:
    payload = {
        "origem": tribunal,
        "send_callback": False,
        "wait": False,
        "documentos_publicos": True,
        "tentativas": 2,
    }
    return _request("POST", f"/processo-tribunal/{numero_cnj}/async", payload)


def resultado_async(busca_id: int) -> dict[str, Any]:
    return _request("GET", f"/async/resultados/{busca_id}")


def aguardar_resultado(busca_id: int, tentativas: int = 24, espera: int = 5) -> dict[str, Any]:
    ultimo = {}
    for _ in range(tentativas):
        ultimo = resultado_async(busca_id)
        if ultimo.get("status") in {"SUCESSO", "ERRO"}:
            return ultimo
        time.sleep(espera)
    return ultimo


def registrar_consulta_inicial(numero_cnj: str, tribunal: str) -> dict[str, Any]:
    processo_id = upsert_processo(numero_cnj, tribunal)

    inicio = consultar_processo(numero_cnj, tribunal)
    busca_id = inicio.get("id")

    final = inicio
    if busca_id:
        final = aguardar_resultado(busca_id)

    salvar_consulta(
        processo_id=processo_id,
        busca_async_id=busca_id,
        status=final.get("status"),
        tipo=final.get("tipo"),
        valor=final.get("valor"),
        resposta_json=json.dumps(final, ensure_ascii=False),
    )

    salvar_movimentacoes_da_consulta(processo_id, final, "consulta_inicial")
    return final


def salvar_movimentacoes_da_consulta(processo_id: int, resultado: dict[str, Any], origem_evento: str) -> int:
    resposta = resultado.get("resposta") or {}
    instancias = resposta.get("instancias") or []
    inseridas = 0

    for instancia in instancias:
        for mov in instancia.get("movimentacoes") or []:
            data_mov = mov.get("data")
            conteudo = mov.get("conteudo") or ""
            chave = hashlib.sha256(f"{processo_id}|{data_mov}|{conteudo}".encode("utf-8")).hexdigest()

            ok = inserir_movimentacao(
                processo_id=processo_id,
                data_movimentacao=data_mov,
                conteudo=conteudo,
                instancia=instancia.get("instancia"),
                orgao_julgador=instancia.get("orgao_julgador"),
                classe=instancia.get("classe"),
                assunto=instancia.get("assunto"),
                origem_evento=origem_evento,
                chave_unica=chave,
                payload_json=json.dumps(
                    {"instancia": instancia, "movimentacao": mov},
                    ensure_ascii=False
                ),
            )
            if ok:
                inseridas += 1

    return inseridas


def criar_monitoramento_tribunal(numero_cnj: str, tribunal: str, frequencia: str = "DIARIA") -> dict[str, Any]:
    payload = {
        "tipo": "UNICO",
        "valor": numero_cnj,
        "tribunal": tribunal,
        "frequencia": frequencia,
    }

    result = _request("POST", "/monitoramento-tribunal", payload)

    atualizar_monitoramento(
        numero_cnj=numero_cnj,
        monitoramento_id=result.get("id"),
        frequencia=result.get("frequencia") or frequencia,
        status_monitoramento=result.get("status"),
    )
    return result