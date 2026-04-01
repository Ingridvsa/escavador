from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

from db import (
    atualizar_detalhes_processo,
    atualizar_monitoramento,
    inserir_movimentacao,
    processo_existe,
    salvar_consulta,
    upsert_processo_basico,
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


def classificar_processo(tribunal: str) -> tuple[str, str | None]:
    tribunal = (tribunal or "").upper().strip()

    if tribunal.startswith("TRT"):
        return "Trabalhista", None

    if tribunal.startswith("TRF"):
        return "Federal", None

    if tribunal.startswith("TJ"):
        uf = tribunal[-2:] if len(tribunal) >= 2 else None
        return "Estadual", uf

    return "Outros", None


def extrair_nome_parte_principal(instancia: dict[str, Any]) -> str | None:
    partes = instancia.get("partes") or []
    if not partes:
        return None

    prioridades = ["RECLAMANTE", "AUTOR", "EXEQUENTE", "REQUERENTE"]

    for prioridade in prioridades:
        for parte in partes:
            if (parte.get("tipo") or "").upper() == prioridade:
                return parte.get("nome")

    for parte in partes:
        if (parte.get("polo") or "").upper() == "ATIVO":
            return parte.get("nome")

    return partes[0].get("nome")


def extrair_detalhes_principais(resultado: dict[str, Any]) -> dict[str, Any]:
    resposta = resultado.get("resposta") or {}
    origem = resposta.get("origem")
    instancias = resposta.get("instancias") or []
    instancia_principal = instancias[0] if instancias else {}

    tipo_processo, subtipo_processo = classificar_processo(origem or "")

    valor_causa = instancia_principal.get("valor_causa")
    if valor_causa is not None:
        valor_causa = str(valor_causa)

    return {
        "tribunal": origem,
        "tipo_processo": tipo_processo,
        "subtipo_processo": subtipo_processo,
        "nome_parte_principal": extrair_nome_parte_principal(instancia_principal),
        "valor_causa": valor_causa,
        "data_distribuicao": instancia_principal.get("data_distribuicao"),
    }


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


def cadastrar_processo_se_nao_existir(numero_cnj: str, tribunal: str) -> dict[str, Any]:
    if processo_existe(numero_cnj):
        raise EscavadorErro("Processo já cadastrado na base.")

    processo_id = upsert_processo_basico(numero_cnj, tribunal)

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

    detalhes = extrair_detalhes_principais(final)
    atualizar_detalhes_processo(
        numero_cnj=numero_cnj,
        tribunal=detalhes.get("tribunal") or tribunal,
        tipo_processo=detalhes.get("tipo_processo"),
        subtipo_processo=detalhes.get("subtipo_processo"),
        nome_parte_principal=detalhes.get("nome_parte_principal"),
        valor_causa=detalhes.get("valor_causa"),
        data_distribuicao=detalhes.get("data_distribuicao"),
        payload_consulta_json=json.dumps(final, ensure_ascii=False),
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
            chave = hashlib.sha256(
                f"{processo_id}|{data_mov}|{conteudo}".encode("utf-8")
            ).hexdigest()

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