from __future__ import annotations

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não definida.")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)


@contextmanager
def get_conn():
    with engine.begin() as conn:
        yield conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS processos (
            id SERIAL PRIMARY KEY,
            numero_cnj TEXT NOT NULL UNIQUE,
            tribunal TEXT NOT NULL,
            tipo_processo TEXT,
            subtipo_processo TEXT,
            nome_parte_principal TEXT,
            valor_causa TEXT,
            data_distribuicao TEXT,
            monitoramento_id BIGINT,
            frequencia TEXT,
            status_monitoramento TEXT,
            payload_consulta_json TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS consultas (
            id SERIAL PRIMARY KEY,
            processo_id BIGINT NOT NULL REFERENCES processos(id),
            busca_async_id BIGINT,
            status TEXT,
            tipo TEXT,
            valor TEXT,
            resposta_json TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS movimentacoes (
            id SERIAL PRIMARY KEY,
            processo_id BIGINT NOT NULL REFERENCES processos(id),
            data_movimentacao TEXT,
            conteudo TEXT NOT NULL,
            instancia TEXT,
            orgao_julgador TEXT,
            classe TEXT,
            assunto TEXT,
            origem_evento TEXT,
            chave_unica TEXT NOT NULL UNIQUE,
            payload_json TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS callbacks_recebidos (
            id SERIAL PRIMARY KEY,
            evento TEXT,
            item_id_externo TEXT,
            payload_json TEXT NOT NULL,
            recebido_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))


def processo_existe(numero_cnj: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM processos WHERE numero_cnj = :numero_cnj LIMIT 1"),
            {"numero_cnj": numero_cnj},
        ).first()
        return row is not None


def upsert_processo_basico(numero_cnj: str, tribunal: str) -> int:
    with get_conn() as conn:
        conn.execute(text("""
            INSERT INTO processos (numero_cnj, tribunal)
            VALUES (:numero_cnj, :tribunal)
            ON CONFLICT (numero_cnj)
            DO UPDATE SET
                tribunal = EXCLUDED.tribunal,
                atualizado_em = CURRENT_TIMESTAMP
        """), {
            "numero_cnj": numero_cnj,
            "tribunal": tribunal,
        })

        row = conn.execute(
            text("SELECT id FROM processos WHERE numero_cnj = :numero_cnj"),
            {"numero_cnj": numero_cnj},
        ).mappings().first()

        return int(row["id"])


def atualizar_detalhes_processo(
    numero_cnj: str,
    tribunal: str,
    tipo_processo: str | None,
    subtipo_processo: str | None,
    nome_parte_principal: str | None,
    valor_causa: str | None,
    data_distribuicao: str | None,
    payload_consulta_json: str | None,
) -> None:
    with get_conn() as conn:
        conn.execute(text("""
            UPDATE processos
            SET tribunal = :tribunal,
                tipo_processo = :tipo_processo,
                subtipo_processo = :subtipo_processo,
                nome_parte_principal = :nome_parte_principal,
                valor_causa = :valor_causa,
                data_distribuicao = :data_distribuicao,
                payload_consulta_json = :payload_consulta_json,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE numero_cnj = :numero_cnj
        """), {
            "numero_cnj": numero_cnj,
            "tribunal": tribunal,
            "tipo_processo": tipo_processo,
            "subtipo_processo": subtipo_processo,
            "nome_parte_principal": nome_parte_principal,
            "valor_causa": valor_causa,
            "data_distribuicao": data_distribuicao,
            "payload_consulta_json": payload_consulta_json,
        })


def atualizar_monitoramento(
    numero_cnj: str,
    monitoramento_id: int | None,
    frequencia: str | None,
    status_monitoramento: str | None,
) -> None:
    with get_conn() as conn:
        conn.execute(text("""
            UPDATE processos
            SET monitoramento_id = :monitoramento_id,
                frequencia = :frequencia,
                status_monitoramento = :status_monitoramento,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE numero_cnj = :numero_cnj
        """), {
            "numero_cnj": numero_cnj,
            "monitoramento_id": monitoramento_id,
            "frequencia": frequencia,
            "status_monitoramento": status_monitoramento,
        })


def salvar_consulta(
    processo_id: int,
    busca_async_id: int | None,
    status: str | None,
    tipo: str | None,
    valor: str | None,
    resposta_json: str,
) -> None:
    with get_conn() as conn:
        conn.execute(text("""
            INSERT INTO consultas (
                processo_id, busca_async_id, status, tipo, valor, resposta_json
            ) VALUES (
                :processo_id, :busca_async_id, :status, :tipo, :valor, :resposta_json
            )
        """), {
            "processo_id": processo_id,
            "busca_async_id": busca_async_id,
            "status": status,
            "tipo": tipo,
            "valor": valor,
            "resposta_json": resposta_json,
        })


def inserir_movimentacao(
    processo_id: int,
    data_movimentacao: str | None,
    conteudo: str,
    instancia: str | None,
    orgao_julgador: str | None,
    classe: str | None,
    assunto: str | None,
    origem_evento: str | None,
    chave_unica: str,
    payload_json: str,
) -> bool:
    try:
        with get_conn() as conn:
            conn.execute(text("""
                INSERT INTO movimentacoes (
                    processo_id, data_movimentacao, conteudo, instancia,
                    orgao_julgador, classe, assunto, origem_evento,
                    chave_unica, payload_json
                ) VALUES (
                    :processo_id, :data_movimentacao, :conteudo, :instancia,
                    :orgao_julgador, :classe, :assunto, :origem_evento,
                    :chave_unica, :payload_json
                )
            """), {
                "processo_id": processo_id,
                "data_movimentacao": data_movimentacao,
                "conteudo": conteudo,
                "instancia": instancia,
                "orgao_julgador": orgao_julgador,
                "classe": classe,
                "assunto": assunto,
                "origem_evento": origem_evento,
                "chave_unica": chave_unica,
                "payload_json": payload_json,
            })
        return True
    except Exception:
        return False


def salvar_callback(evento: str | None, item_id_externo: str | None, payload_json: str) -> None:
    with get_conn() as conn:
        conn.execute(text("""
            INSERT INTO callbacks_recebidos (evento, item_id_externo, payload_json)
            VALUES (:evento, :item_id_externo, :payload_json)
        """), {
            "evento": evento,
            "item_id_externo": item_id_externo,
            "payload_json": payload_json,
        })


def listar_processos():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT *
            FROM processos
            ORDER BY tipo_processo NULLS LAST, subtipo_processo NULLS LAST, numero_cnj
        """)).mappings().all()
        return [dict(r) for r in rows]


def buscar_processo_por_numero(numero_cnj: str):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT *
            FROM processos
            WHERE numero_cnj = :numero_cnj
        """), {"numero_cnj": numero_cnj}).mappings().first()

        return dict(row) if row else None


def listar_movimentacoes(numero_cnj: str | None = None):
    with engine.connect() as conn:
        if numero_cnj:
            rows = conn.execute(text("""
                SELECT m.*, p.numero_cnj, p.tribunal
                FROM movimentacoes m
                JOIN processos p ON p.id = m.processo_id
                WHERE p.numero_cnj = :numero_cnj
                ORDER BY m.id DESC
            """), {"numero_cnj": numero_cnj}).mappings().all()
        else:
            rows = conn.execute(text("""
                SELECT m.*, p.numero_cnj, p.tribunal
                FROM movimentacoes m
                JOIN processos p ON p.id = m.processo_id
                ORDER BY m.id DESC
            """)).mappings().all()

        return [dict(r) for r in rows]


def listar_callbacks(limit: int = 100):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT *
            FROM callbacks_recebidos
            ORDER BY id DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]