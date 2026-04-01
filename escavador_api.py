from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

from db import (
    inserir_movimentacao,
    salvar_consulta,
    upsert_processo,
    atualizar_monitoramento_processo,
)

load_dotenv()

API_TOKEN = os.getenv("ESCAVADOR_API_TOKEN")
BASE_URL = os.getenv("ESCAVADOR_BASE_URL", "https://api.escavador.com/api/v1")
TIMEOUT = 30


class EscavadorAPIError(Exception):
    pass


def _headers(content_type_json: bool = True) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "X-Requested-With": "XMLHttpRequest",
    }
    if content_type_json:
        headers["Content-Type"] = "application/json"
    return headers


def _request(
    method: str,
    endpoint: str,
    *,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    content_type_json: bool = True,
) -> dict[str, Any]:
    if not API_TOKEN:
        raise EscavadorAPIError("ESCAVADOR_API_TOKEN não encontrado no .env")

    url = f"{BASE_URL}{endpoint}"

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=_headers(content_type_json=content_type_json),
            json=payload,
            params=params,
            timeout=TIMEOUT,
        )
    except requests.RequestException as e:
        raise EscavadorAPIError(f"Erro de conexão: {e}") from e

    try:
        data = response.json()
    except ValueError:
        raise EscavadorAPIError(
            f"Resposta inválida. Status={response.status_code} Corpo={response.text[:500]}"
        )

    if response.status_code >= 400:
        raise EscavadorAPIError(
            f"Erro HTTP {response.status_code}: {json.dumps(data, ensure_ascii=False)}"
        )

    return data


def detalhes_origem(sigla: str) -> dict[str, Any]:
    return _request("GET", f"/tribunal/origens/{sigla}", content_type_json=False)


def iniciar_busca_processo(numero_cnj: str, origem: str) -> dict[str, Any]:
    endpoint = f"/processo-tribunal/{numero_cnj}/async"
    payload = {
        "origem": origem,
        "send_callback": 0,
        "wait": 0,
        "documentos_publicos": 1,
    }
    return _request("POST", endpoint, payload=payload)


def consultar_resultado_async(busca_id: int) -> dict[str, Any]:
    return _request("GET", f"/async/resultados/{busca_id}", content_type_json=False)


def aguardar_resultado(busca_id: int, tentativas: int = 24, espera: int = 5) -> dict[str, Any]:
    ultimo = {}
    for _ in range(tentativas):
        ultimo = consultar_resultado_async(busca_id)
        status = ultimo.get("status")
        if status in {"SUCESSO", "ERRO"}:
            return ultimo
        time.sleep(espera)
    return ultimo


def registrar_consulta_inicial(numero_cnj: str, origem: str) -> dict[str, Any]:
    processo_id = upsert_processo(numero_cnj, origem)

    inicial = iniciar_busca_processo(numero_cnj, origem)
    busca_id = inicial.get("id")

    final = inicial
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

    extrair_e_salvar_movimentacoes_resultado(processo_id, final, origem_evento="consulta_inicial")
    return final


def extrair_e_salvar_movimentacoes_resultado(
    processo_id: int,
    resultado: dict[str, Any],
    origem_evento: str,
) -> int:
    resposta = resultado.get("resposta") or {}
    instancias = resposta.get("instancias") or []
    inseridas = 0

    for instancia in instancias:
        for mov in instancia.get("movimentacoes") or []:
            data = mov.get("data")
            conteudo = mov.get("conteudo") or ""
            hash_unico = hashlib.sha256(
                f"{processo_id}|{data}|{conteudo}".encode("utf-8")
            ).hexdigest()

            ok = inserir_movimentacao(
                processo_id=processo_id,
                data_movimentacao=data,
                conteudo=conteudo,
                instancia=instancia.get("instancia"),
                orgao_julgador=instancia.get("orgao_julgador"),
                classe=instancia.get("classe"),
                assunto=instancia.get("assunto"),
                origem_evento=origem_evento,
                hash_unico=hash_unico,
                payload_json=json.dumps(
                    {"instancia": instancia, "movimentacao": mov},
                    ensure_ascii=False
                ),
            )
            if ok:
                inseridas += 1

    return inseridas


def criar_monitoramento(numero_cnj: str, origem: str, frequencia: str = "DIARIA") -> dict[str, Any]:
    payload = {
        "tipo": "UNICO",
        "valor": numero_cnj,
        "origens": [origem],
        "frequencia": frequencia,
        "incluir_docpub": True,
        "incluir_autos": False,
    }
    result = _request("POST", "/monitoramentos-tribunal", payload=payload)

    atualizar_monitoramento_processo(
        numero_cnj=numero_cnj,
        monitoramento_id=result.get("id"),
        ativo=True,
        frequencia=result.get("frequencia") or frequencia,
        status_monitoramento=result.get("status"),
    )
    return result


def listar_monitoramentos() -> dict[str, Any]:
    return _request("GET", "/monitoramentos-tribunal", content_type_json=False)