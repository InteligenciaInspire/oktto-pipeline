from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st

try:
    from google.oauth2.credentials import Credentials as UserCredentials
    from google_auth_oauthlib.flow import Flow
except ImportError:
    UserCredentials = Any
    Flow = None

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClientOAuth
from src.config import OkttoSettings
from src.extract.additional_fields import fetch_additional_fields
from src.extract.funnels import fetch_funnels
from src.extract.leads import fetch_leads
from src.extract.sales import fetch_sales
from src.extract.teams import fetch_teams
from src.extract.users import fetch_users
from src.main import JOBS, run_job_with_clients
from src.transform.normalize_leads import normalize_leads
from src.transform.normalize_sales import normalize_sales


PROJECT_ROOT = None  # not used in cloud mode
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def _google_oauth_available() -> bool:
    return Flow is not None


def _require_google_oauth() -> None:
    if not _google_oauth_available():
        raise RuntimeError(
            "OAuth Google indisponivel neste deploy. "
            "Verifique a instalacao de google-auth e google-auth-oauthlib."
        )



def _secret(key: str, fallback: str = "") -> str:
    try:
        return str(st.secrets.get(key, fallback))
    except Exception:
        return fallback


DATASET_EXTRACTORS: dict[str, Callable[[OkttoClient], list[dict]]] = {
    "leads": fetch_leads,
    "sales": fetch_sales,
    "users": fetch_users,
    "teams": fetch_teams,
    "funnels": fetch_funnels,
    "additional_fields": fetch_additional_fields,
}


def _load_env_defaults() -> dict[str, str]:
    def _val(key: str, fallback: str = "") -> str:
        return _secret(key, fallback)

    return {
        "OKTTO_API_BASE_URL": _val("OKTTO_API_BASE_URL", "https://api.oktto.com.br/v1"),
        "GOOGLE_OAUTH_CLIENT_ID": _val("GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": _val("GOOGLE_OAUTH_CLIENT_SECRET"),
        "GOOGLE_OAUTH_REDIRECT_URI": _val("GOOGLE_OAUTH_REDIRECT_URI"),
    }



def _normalize_dataset(dataset: str, items: list[dict]) -> pd.DataFrame:
    if dataset == "leads":
        return normalize_leads(items)
    if dataset == "sales":
        return normalize_sales(items)
    if not items:
        return pd.DataFrame()
    return pd.json_normalize(items, sep="__")


def _oauth_client_config(client_id: str, client_secret: str, redirect_uri: str) -> dict:
    return {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect_uri],
        }
    }


def _make_user_credentials(payload: dict) -> UserCredentials:
    _require_google_oauth()
    return UserCredentials(
        token=payload.get("token"),
        refresh_token=payload.get("refresh_token"),
        token_uri=payload.get("token_uri"),
        client_id=payload.get("client_id"),
        client_secret=payload.get("client_secret"),
        scopes=payload.get("scopes"),
    )


def _oauth_login_panel(defaults: dict[str, str]) -> None:
    st.markdown("#### Login Google (OAuth)")

    if not _google_oauth_available():
        st.warning("Login Google indisponivel neste deploy. O modo de extracao CSV continua funcionando.")
        return

    client_id = st.text_input(
        "Google OAuth Client ID",
        value=defaults["GOOGLE_OAUTH_CLIENT_ID"],
        type="password",
        help="Pode ser fixo no .env da aplicacao ou informado aqui.",
    ).strip()
    client_secret = st.text_input(
        "Google OAuth Client Secret",
        value=defaults["GOOGLE_OAUTH_CLIENT_SECRET"],
        type="password",
    ).strip()
    redirect_uri = st.text_input(
        "Google OAuth Redirect URI",
        value=defaults["GOOGLE_OAUTH_REDIRECT_URI"],
        help="Exemplo Streamlit Cloud: https://seu-app.streamlit.app",
    ).strip()

    if not client_id or not client_secret or not redirect_uri:
        st.warning("Preencha Client ID, Client Secret e Redirect URI para habilitar login Google.")
        return

    query_params = st.query_params
    code = query_params.get("code")
    state_in_url = query_params.get("state")

    if "google_user_creds" not in st.session_state:
        flow = Flow.from_client_config(
            _oauth_client_config(client_id, client_secret, redirect_uri),
            scopes=OAUTH_SCOPES,
        )
        flow.redirect_uri = redirect_uri
        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
        )
        st.session_state["oauth_state"] = state
        st.link_button("Entrar com Google", authorization_url, use_container_width=True)

    if code and "google_user_creds" not in st.session_state:
        try:
            flow = Flow.from_client_config(
                _oauth_client_config(client_id, client_secret, redirect_uri),
                scopes=OAUTH_SCOPES,
                state=st.session_state.get("oauth_state") or state_in_url,
            )
            flow.redirect_uri = redirect_uri
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["google_user_creds"] = {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
            }
            st.query_params.clear()
            st.success("Login Google concluido para esta sessao.")
        except Exception as exc:
            st.error(f"Falha no login Google: {exc}")

    if "google_user_creds" in st.session_state:
        st.success("Google conectado na sessao atual.")
        if st.button("Desconectar Google", use_container_width=True):
            st.session_state.pop("google_user_creds", None)
            st.session_state.pop("oauth_state", None)
            st.info("Sessao Google removida.")


def _run_public_job_to_user_sheets(
    job_name: str,
    oktto_base_url: str,
    oktto_token: str,
    spreadsheet_id: str,
) -> None:
    _require_google_oauth()
    creds_payload = st.session_state.get("google_user_creds")
    if not creds_payload:
        raise RuntimeError("Faca login Google antes de executar o job.")

    oauth_creds = _make_user_credentials(creds_payload)
    sheets_client = SheetsClientOAuth(spreadsheet_id=spreadsheet_id, credentials=oauth_creds)
    oktto_client = OkttoClient(OkttoSettings(base_url=oktto_base_url, token=oktto_token))
    run_job_with_clients(job_name, oktto_client, sheets_client)


def _public_extract(dataset: str, base_url: str, token: str, page_size: int) -> pd.DataFrame:
    client = OkttoClient(OkttoSettings(base_url=base_url, token=token, page_size=page_size))
    extractor = DATASET_EXTRACTORS[dataset]
    items = extractor(client)
    return _normalize_dataset(dataset, items)


def _how_it_works() -> None:
    with st.expander("Como funciona?", expanded=False):
        st.markdown(
            """
**O que e este app?**

Conecta sua conta da **Oktto CRM** e extrai dados comerciais — leads, vendas, usuarios, funis e mais.
Voce pode baixar os dados como **CSV** ou enviar diretamente para o seu **Google Sheets**.

---

**O que voce precisa?**

- **Token da Oktto** — encontrado em _Configuracoes > Integracoes > API_ dentro da sua conta Oktto.
- **Spreadsheet ID** (opcional, so para enviar ao Sheets) — e o trecho da URL da planilha entre `/d/` e `/edit`. Exemplo:
  `https://docs.google.com/spreadsheets/d/**SEU_ID_AQUI**/edit`

---

**Opcao 1 — Baixar CSV**

1. Informe seu token da Oktto
2. Escolha o dataset (leads, vendas, usuarios...)
3. Clique em **Extrair** e depois em **Baixar CSV**

---

**Opcao 2 — Enviar para Google Sheets**

1. Faca login com sua conta Google (botao abaixo)
2. Informe seu token da Oktto e o ID da planilha de destino
3. Escolha o job e clique em **Executar e enviar para Sheets**

Os dados serao escritos em abas separadas dentro da sua planilha.
Nenhum token ou dado seu e salvo neste servidor.

---

**Jobs disponiveis**

| Job | O que faz |
|---|---|
| `sync_dimensions` | Funis, etapas, usuarios, equipes e campos adicionais |
| `sync_leads` | Todos os leads com campos normalizados |
| `sync_sales` | Todas as vendas com campos normalizados |
| `sync_full` | Tudo acima + view comercial resumida |
"""
        )


def _section_extract_csv(defaults: dict[str, str]) -> None:
    st.subheader("Extrair dados para CSV")

    with st.form("extract_form"):
        base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"])
        token = st.text_input("Token da Oktto", type="password", help="Configuracoes > Integracoes > API na sua conta Oktto")
        dataset = st.selectbox(
            "Dataset",
            options=list(DATASET_EXTRACTORS.keys()),
            format_func=lambda x: {
                "leads": "Leads",
                "sales": "Vendas",
                "users": "Usuarios",
                "teams": "Equipes",
                "funnels": "Funis",
                "additional_fields": "Campos adicionais",
            }.get(x, x),
        )
        page_size = st.number_input("Itens por pagina", min_value=10, max_value=500, value=100, step=10)
        submit = st.form_submit_button("Extrair", use_container_width=True, type="primary")

    if submit:
        if not token.strip():
            st.error("Informe o token da Oktto para extrair.")
            return
        try:
            with st.spinner("Extraindo dados..."):
                df = _public_extract(dataset, base_url.strip(), token.strip(), int(page_size))
            st.success(f"Extracao concluida — {len(df)} linhas")
            st.dataframe(df.head(200), use_container_width=True)
            st.download_button(
                label="Baixar CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"oktto_{dataset}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except Exception as exc:
            st.error(f"Falha na extracao: {exc}")


def _section_send_to_sheets(defaults: dict[str, str]) -> None:
    st.subheader("Enviar para Google Sheets")

    _oauth_login_panel(defaults)

    st.divider()

    with st.form("sheets_form"):
        base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"], key="sh_base")
        token = st.text_input("Token da Oktto", type="password", key="sh_token")
        spreadsheet_id = st.text_input(
            "Spreadsheet ID de destino",
            key="sh_id",
            help="Trecho da URL da planilha entre /d/ e /edit",
        )
        job_labels = {
            "sync_dimensions": "sync_dimensions — funis, etapas, usuarios, equipes",
            "sync_leads": "sync_leads — leads",
            "sync_sales": "sync_sales — vendas",
            "sync_full": "sync_full — tudo + view comercial",
        }
        selected_job = st.selectbox(
            "Job",
            options=list(JOBS.keys()),
            format_func=lambda x: job_labels.get(x, x),
            key="sh_job",
        )
        send = st.form_submit_button("Executar e enviar para Sheets", use_container_width=True, type="primary")

    if send:
        if not token.strip():
            st.error("Informe seu token Oktto.")
        elif not spreadsheet_id.strip():
            st.error("Informe o Spreadsheet ID.")
        else:
            try:
                with st.spinner("Executando job e escrevendo no Sheets..."):
                    _run_public_job_to_user_sheets(
                        job_name=selected_job,
                        oktto_base_url=base_url.strip(),
                        oktto_token=token.strip(),
                        spreadsheet_id=spreadsheet_id.strip(),
                    )
                st.success(f"Concluido! Job '{selected_job}' enviado para a planilha.")
            except Exception as exc:
                st.error(f"Falha: {exc}")


def main() -> None:
    st.set_page_config(page_title="Oktto Pipeline", page_icon="📊", layout="centered")
    st.title("📊 Oktto Pipeline")
    st.caption("Extraia dados do seu CRM Oktto — baixe como CSV ou envie direto para o Google Sheets.")

    _how_it_works()

    defaults = _load_env_defaults()

    st.divider()

    tab_csv, tab_sheets = st.tabs(["📥 Extrair CSV", "📤 Enviar para Google Sheets"])

    with tab_csv:
        _section_extract_csv(defaults)

    with tab_sheets:
        _section_send_to_sheets(defaults)


if __name__ == "__main__":
    main()
