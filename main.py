from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("ESCAVADOR_API_TOKEN")
BASE_URL = "https://api.escavador.com/api/v1"

NUMERO_PROCESSO = "0000074-20.2026.5.06.0012"
TRIBUNAL = "TRT-6"
FREQUENCIA_MONITORAMENTO = "DIARIA"

TIMEOUT = 30
INTERVALO_CONSULTA = 5
MAX_TENTATIVAS = 24


def validar_config() -> None:
    if not API_TOKEN:
        raise ValueError(
            "Token não encontrado. Crie um arquivo .env com:\n"
            "ESCAVADOR_API_TOKEN=seu_token_aqui"
        )


def get_headers(content_type_json: bool = True) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "X-Requested-With": "XMLHttpRequest",
    }
    if content_type_json:
        headers["Content-Type"] = "application/json"
    return headers


def print_json(titulo: str, dados: Any) -> None:
    print("\n" + "=" * 100)
    print(titulo)
    print("=" * 100)
    print(json.dumps(dados, ensure_ascii=False, indent=2))


def fazer_request(
    metodo: str,
    endpoint: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    content_type_json: bool = True,
) -> Dict[str, Any]:
    url = f"{BASE_URL}{endpoint}"

    try:
        resposta = requests.request(
            method=metodo,
            url=url,
            headers=get_headers(content_type_json=content_type_json),
            params=params,
            json=payload,
            timeout=TIMEOUT,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"Erro de conexão com a API: {e}") from e

    creditos = resposta.headers.get("Creditos-Utilizados")
    if creditos is not None:
        print(f"↳ Créditos utilizados: {creditos}")

    try:
        dados = resposta.json()
    except ValueError:
        raise RuntimeError(
            f"Resposta não retornou JSON. Status={resposta.status_code} | "
            f"Corpo={resposta.text[:500]}"
        )

    if resposta.status_code >= 400:
        raise RuntimeError(
            f"Erro HTTP {resposta.status_code} em {endpoint}: "
            f"{json.dumps(dados, ensure_ascii=False)}"
        )

    return dados


def detalhes_origem(sigla: str) -> Dict[str, Any]:
    return fazer_request("GET", f"/tribunal/origens/{sigla}", content_type_json=False)


def iniciar_busca_processo(numero_processo: str, tribunal: str) -> Dict[str, Any]:
    """
    Rota confirmada no PDF:
    POST api/v1/processo-tribunal/{numero}/async
    """
    endpoint = f"/processo-tribunal/{numero_processo}/async"

    payload = {
        "origem": tribunal,
        "send_callback": 0,
        "wait": 0,
        "documentos_publicos": 1,
        # "autos": 0,
        # "usuario": "",
        # "senha": "",
    }

    return fazer_request("POST", endpoint, payload=payload)


def consultar_resultado_async(resultado_id: int) -> Dict[str, Any]:
    return fazer_request("GET", f"/async/resultados/{resultado_id}", content_type_json=False)


def aguardar_resultado(resultado_id: int) -> Dict[str, Any]:
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        print(f"\n[Polling] Tentativa {tentativa}/{MAX_TENTATIVAS}")
        resultado = consultar_resultado_async(resultado_id)
        status = resultado.get("status")
        print(f"↳ Status: {status}")

        if status in {"SUCESSO", "ERRO"}:
            return resultado

        time.sleep(INTERVALO_CONSULTA)

    raise TimeoutError("Tempo esgotado aguardando o resultado da busca assíncrona.")


def extrair_movimentacoes(resultado: Dict[str, Any]) -> List[Dict[str, Any]]:
    resposta = resultado.get("resposta") or {}
    instancias = resposta.get("instancias") or []
    itens: List[Dict[str, Any]] = []

    for instancia in instancias:
        for mov in instancia.get("movimentacoes") or []:
            itens.append({
                "data": mov.get("data"),
                "conteudo": mov.get("conteudo"),
                "instancia": instancia.get("instancia"),
                "orgao_julgador": instancia.get("orgao_julgador"),
                "classe": instancia.get("classe"),
                "assunto": instancia.get("assunto"),
            })

    return itens


def mostrar_resumo_resultado(resultado: Dict[str, Any]) -> None:
    print("\n" + "=" * 100)
    print("RESUMO DA CONSULTA")
    print("=" * 100)

    print(f"Status: {resultado.get('status')}")
    print(f"Tipo: {resultado.get('tipo')}")
    print(f"Valor: {resultado.get('valor')}")

    resposta = resultado.get("resposta")
    if not isinstance(resposta, dict):
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
        return

    print(f"Número único: {resposta.get('numero_unico')}")
    print(f"Origem: {resposta.get('origem')}")

    instancias = resposta.get("instancias") or []
    print(f"Instâncias encontradas: {len(instancias)}")

    for i, instancia in enumerate(instancias, start=1):
        print(f"\nInstância {i}")
        print(f"  Sistema: {instancia.get('sistema')}")
        print(f"  Instância: {instancia.get('instancia')}")
        print(f"  Órgão julgador: {instancia.get('orgao_julgador')}")
        print(f"  Classe: {instancia.get('classe')}")
        print(f"  Assunto: {instancia.get('assunto')}")
        print(f"  Data distribuição: {instancia.get('data_distribuicao')}")
        print(f"  Última atualização: {instancia.get('last_update_time')}")
        print(f"  Arquivado: {instancia.get('arquivado')}")

        partes = instancia.get("partes") or []
        print(f"  Partes: {len(partes)}")
        for parte in partes[:10]:
            print(f"    - {parte.get('tipo')}: {parte.get('nome')}")

        movs = instancia.get("movimentacoes") or []
        print(f"  Movimentações: {len(movs)}")
        for mov in movs[:10]:
            print(f"    - {mov.get('data')} | {mov.get('conteudo')}")


def criar_monitoramento(numero_processo: str, tribunal: str) -> Dict[str, Any]:
    payload = {
        "tipo": "UNICO",
        "valor": numero_processo,
        "origens": [tribunal],
        "frequencia": FREQUENCIA_MONITORAMENTO,
        "incluir_docpub": True,
        "incluir_autos": False,
    }
    return fazer_request("POST", "/monitoramentos-tribunal", payload=payload)


def listar_monitoramentos() -> Dict[str, Any]:
    return fazer_request("GET", "/monitoramentos-tribunal", content_type_json=False)


def consultar_monitoramento(monitoramento_id: int) -> Dict[str, Any]:
    return fazer_request(
        "GET",
        f"/monitoramentos-tribunal/{monitoramento_id}",
        content_type_json=False,
    )


def main() -> None:
    validar_config()

    print("Iniciando teste Escavador")
    print(f"Processo: {NUMERO_PROCESSO}")
    print(f"Tribunal: {TRIBUNAL}")
    print(f"Frequência monitoramento: {FREQUENCIA_MONITORAMENTO}")

    try:
        origem = detalhes_origem(TRIBUNAL)
        print_json("DETALHES DA ORIGEM", origem)
    except Exception as e:
        print(f"[Aviso] Não foi possível consultar detalhes do tribunal: {e}")

    try:
        busca = iniciar_busca_processo(NUMERO_PROCESSO, TRIBUNAL)
        print_json("BUSCA INICIADA", busca)
    except Exception as e:
        print(f"[Erro] Falha ao iniciar busca: {e}")
        sys.exit(1)

    busca_id = busca.get("id")
    if not busca_id:
        print("[Erro] A resposta da busca não trouxe 'id'.")
        sys.exit(1)

    try:
        resultado = aguardar_resultado(busca_id)
        print_json("RESULTADO FINAL", resultado)
    except Exception as e:
        print(f"[Erro] Falha ao aguardar resultado: {e}")
        sys.exit(1)

    mostrar_resumo_resultado(resultado)

    movimentacoes = extrair_movimentacoes(resultado)
    print("\n" + "=" * 100)
    print("MOVIMENTAÇÕES EXTRAÍDAS")
    print("=" * 100)
    if movimentacoes:
        for i, mov in enumerate(movimentacoes, start=1):
            print(f"{i:02d}. {mov['data']} | {mov['conteudo']}")
    else:
        print("Nenhuma movimentação encontrada.")

    try:
        monitoramento = criar_monitoramento(NUMERO_PROCESSO, TRIBUNAL)
        print_json("MONITORAMENTO CRIADO", monitoramento)
    except Exception as e:
        print(f"[Erro] Falha ao criar monitoramento: {e}")
        sys.exit(1)

    monitoramento_id = monitoramento.get("id")
    if monitoramento_id:
        try:
            detalhe_monitor = consultar_monitoramento(monitoramento_id)
            print_json("DETALHE DO MONITORAMENTO", detalhe_monitor)
        except Exception as e:
            print(f"[Aviso] Não foi possível consultar o monitoramento: {e}")

    try:
        lista = listar_monitoramentos()
        print_json("LISTA DE MONITORAMENTOS", lista)
    except Exception as e:
        print(f"[Aviso] Não foi possível listar monitoramentos: {e}")


if __name__ == "__main__":
    main()