from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "escavador.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS processos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_cnj TEXT NOT NULL UNIQUE,
            origem TEXT NOT NULL,
            monitoramento_id INTEGER,
            monitoramento_ativo INTEGER NOT NULL DEFAULT 0,
            frequencia TEXT,
            status_monitoramento TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processo_id INTEGER NOT NULL,
            busca_async_id INTEGER,
            status TEXT,
            tipo TEXT,
            valor TEXT,
            resposta_json TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (processo_id) REFERENCES processos(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS movimentacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processo_id INTEGER NOT NULL,
            data_movimentacao TEXT,
            conteudo TEXT NOT NULL,
            instancia TEXT,
            orgao_julgador TEXT,
            classe TEXT,
            assunto TEXT,
            origem_evento TEXT,
            hash_unico TEXT NOT NULL UNIQUE,
            payload_json TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (processo_id) REFERENCES processos(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS callbacks_recebidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento TEXT,
            item_id_externo TEXT,
            status_callback TEXT,
            payload_json TEXT NOT NULL,
            recebido_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)


def upsert_processo(numero_cnj: str, origem: str) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO processos (numero_cnj, origem)
            VALUES (?, ?)
            ON CONFLICT(numero_cnj) DO UPDATE SET
                origem=excluded.origem,
                atualizado_em=CURRENT_TIMESTAMP
        """, (numero_cnj, origem))
        cur.execute("SELECT id FROM processos WHERE numero_cnj = ?", (numero_cnj,))
        row = cur.fetchone()
        return int(row["id"])


def atualizar_monitoramento_processo(
    numero_cnj: str,
    monitoramento_id: int | None,
    ativo: bool,
    frequencia: str | None,
    status_monitoramento: str | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute("""
            UPDATE processos
            SET monitoramento_id = ?,
                monitoramento_ativo = ?,
                frequencia = ?,
                status_monitoramento = ?,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE numero_cnj = ?
        """, (
            monitoramento_id,
            1 if ativo else 0,
            frequencia,
            status_monitoramento,
            numero_cnj,
        ))


def salvar_consulta(
    processo_id: int,
    busca_async_id: int | None,
    status: str | None,
    tipo: str | None,
    valor: str | None,
    resposta_json: str,
) -> None:
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO consultas (
                processo_id, busca_async_id, status, tipo, valor, resposta_json
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            processo_id, busca_async_id, status, tipo, valor, resposta_json
        ))


def inserir_movimentacao(
    processo_id: int,
    data_movimentacao: str | None,
    conteudo: str,
    instancia: str | None,
    orgao_julgador: str | None,
    classe: str | None,
    assunto: str | None,
    origem_evento: str | None,
    hash_unico: str,
    payload_json: str,
) -> bool:
    try:
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO movimentacoes (
                    processo_id, data_movimentacao, conteudo, instancia,
                    orgao_julgador, classe, assunto, origem_evento,
                    hash_unico, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                processo_id, data_movimentacao, conteudo, instancia,
                orgao_julgador, classe, assunto, origem_evento,
                hash_unico, payload_json
            ))
        return True
    except sqlite3.IntegrityError:
        return False


def salvar_callback(evento: str | None, item_id_externo: str | None, status_callback: str | None, payload_json: str) -> None:
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO callbacks_recebidos (evento, item_id_externo, status_callback, payload_json)
            VALUES (?, ?, ?, ?)
        """, (evento, item_id_externo, status_callback, payload_json))


def listar_processos():
    with get_conn() as conn:
        return conn.execute("""
            SELECT *
            FROM processos
            ORDER BY atualizado_em DESC, id DESC
        """).fetchall()


def listar_movimentacoes_processo(numero_cnj: str):
    with get_conn() as conn:
        return conn.execute("""
            SELECT m.*, p.numero_cnj, p.origem
            FROM movimentacoes m
            JOIN processos p ON p.id = m.processo_id
            WHERE p.numero_cnj = ?
            ORDER BY COALESCE(m.data_movimentacao, m.criado_em) DESC, m.id DESC
        """, (numero_cnj,)).fetchall()


def listar_todas_movimentacoes(limit: int = 200):
    with get_conn() as conn:
        return conn.execute("""
            SELECT m.*, p.numero_cnj, p.origem
            FROM movimentacoes m
            JOIN processos p ON p.id = m.processo_id
            ORDER BY m.id DESC
            LIMIT ?
        """, (limit,)).fetchall()


def listar_callbacks(limit: int = 100):
    with get_conn() as conn:
        return conn.execute("""
            SELECT *
            FROM callbacks_recebidos
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()