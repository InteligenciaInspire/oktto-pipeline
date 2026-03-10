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


DATASET_LABELS: dict[str, str] = {
    "leads": "Leads",
    "sales": "Vendas",
    "users": "Usuarios",
    "teams": "Equipes",
    "funnels": "Funis",
    "additional_fields": "Campos adicionais",
}

JOB_LABELS: dict[str, str] = {
    "sync_dimensions": "Dimensoes — funis, etapas, usuarios, equipes",
    "sync_leads": "Leads — todos os leads",
    "sync_sales": "Vendas — todas as vendas",
    "sync_full": "Completo — tudo + view comercial",
}


def _token_input() -> str:
    """Single token field persisted in session state across reruns."""
    token = st.text_input(
        "Token da Oktto",
        value=st.session_state.get("oktto_token", ""),
        type="password",
        placeholder="Cole aqui o token da API Oktto",
        help="Encontrado em Configuracoes › Integracoes › API dentro da sua conta Oktto.",
        key="_token_widget",
    )
    st.session_state["oktto_token"] = token
    return token.strip()


def _how_it_works() -> None:
    with st.expander("ℹ️ Como funciona?", expanded=False):
        st.markdown(
            """
Este app conecta ao seu **CRM Oktto** e extrai dados comerciais.
Voce pode **baixar como CSV** ou **enviar direto para o Google Sheets**.

**O que voce precisa:**
- **Token da Oktto** — em _Configuracoes › Integracoes › API_ na sua conta Oktto
- **Spreadsheet ID** _(so para enviar ao Sheets)_ — trecho da URL entre `/d/` e `/edit`

**Dados disponíveis:** Leads · Vendas · Usuarios · Equipes · Funis · Campos adicionais

Nenhum token ou dado seu e salvo neste servidor.
"""
        )


def _section_extract_csv(token: str, defaults: dict[str, str]) -> None:
    dataset = st.selectbox(
        "Que dados voce quer extrair?",
        options=list(DATASET_EXTRACTORS.keys()),
        format_func=lambda x: DATASET_LABELS.get(x, x),
    )

    with st.expander("Opcoes avancadas", expanded=False):
        base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"], key="csv_base")
        page_size = st.number_input("Itens por pagina", min_value=10, max_value=500, value=100, step=10)

    if st.button("⬇️ Extrair", use_container_width=True, type="primary", key="btn_extract"):
        if not token:
            st.error("Informe seu token da Oktto no campo acima.")
            return
        try:
            with st.spinner("Extraindo dados..."):
                df = _public_extract(dataset, base_url, token, int(page_size))
            st.success(f"{len(df)} registros extraídos")
            st.dataframe(df.head(200), use_container_width=True)
            st.download_button(
                label="⬇️ Baixar CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"oktto_{dataset}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except Exception as exc:
            st.error(f"Falha na extracao: {exc}")


def _oauth_login_panel(defaults: dict[str, str]) -> None:
    if not _google_oauth_available():
        st.warning("Login Google indisponivel neste deploy.")
        return

    client_id = defaults["GOOGLE_OAUTH_CLIENT_ID"]
    client_secret = defaults["GOOGLE_OAUTH_CLIENT_SECRET"]
    redirect_uri = defaults["GOOGLE_OAUTH_REDIRECT_URI"]

    if not client_id or not client_secret or not redirect_uri:
        with st.expander("Configurar credenciais OAuth Google", expanded=True):
            st.caption("Preencha os campos abaixo para habilitar o login com Google.")
            client_id = st.text_input("Google OAuth Client ID", type="password", key="oa_cid").strip()
            client_secret = st.text_input("Google OAuth Client Secret", type="password", key="oa_cs").strip()
            redirect_uri = st.text_input(
                "Redirect URI",
                placeholder="https://elt-oktto.streamlit.app",
                key="oa_ru",
                help="URL exata deste app — deve estar cadastrada no Google Cloud Console.",
            ).strip()

    if not client_id or not client_secret or not redirect_uri:
        return

    # Already logged in – show status and disconnect button
    if "google_user_creds" in st.session_state:
        st.success("✅ Google conectado")
        if st.button("Desconectar Google", key="btn_disconnect"):
            for k in ("google_user_creds", "oauth_state", "oauth_code_used"):
                st.session_state.pop(k, None)
            st.query_params.clear()
            st.rerun()
        return

    code = st.query_params.get("code")
    state_in_url = st.query_params.get("state")

    # Handle OAuth callback: exchange code exactly once
    if code and not st.session_state.get("oauth_code_used"):
        st.session_state["oauth_code_used"] = True
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
                "scopes": list(creds.scopes) if creds.scopes else [],
            }
            st.query_params.clear()
            st.rerun()
        except Exception as exc:
            st.session_state.pop("oauth_code_used", None)
            st.query_params.clear()
            st.error(f"Falha no login Google: {exc}. Tente novamente.")
        return

    # Show login button (only when no pending code in URL)
    if not code:
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
        st.link_button("🔑 Entrar com Google", authorization_url, use_container_width=True)


def _section_send_to_sheets(token: str, defaults: dict[str, str]) -> None:
    _oauth_login_panel(defaults)

    if "google_user_creds" not in st.session_state:
        return

    st.divider()

    spreadsheet_url_or_id = st.text_input(
        "URL ou ID da planilha de destino",
        placeholder="https://docs.google.com/spreadsheets/d/SEU_ID/edit  ou  cole o ID direto",
        help="Cole a URL completa da planilha ou apenas o ID (trecho entre /d/ e /edit).",
        key="sh_url",
    ).strip()

    # Accept full URL or bare ID
    spreadsheet_id = spreadsheet_url_or_id
    if "/spreadsheets/d/" in spreadsheet_url_or_id:
        try:
            spreadsheet_id = spreadsheet_url_or_id.split("/spreadsheets/d/")[1].split("/")[0]
        except IndexError:
            pass

    selected_job = st.selectbox(
        "O que voce quer sincronizar?",
        options=list(JOBS.keys()),
        format_func=lambda x: JOB_LABELS.get(x, x),
        key="sh_job",
    )

    with st.expander("Opcoes avancadas", expanded=False):
        base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"], key="sh_base")

    if st.button("📤 Enviar para Sheets", use_container_width=True, type="primary", key="btn_sheets"):
        if not token:
            st.error("Informe seu token da Oktto no campo acima.")
        elif not spreadsheet_id:
            st.error("Informe a URL ou ID da planilha.")
        else:
            try:
                with st.spinner("Sincronizando dados com o Google Sheets..."):
                    _run_public_job_to_user_sheets(
                        job_name=selected_job,
                        oktto_base_url=base_url,
                        oktto_token=token,
                        spreadsheet_id=spreadsheet_id,
                    )
                st.success(f"✅ Concluido! Dados enviados para a planilha.")
            except Exception as exc:
                st.error(f"Falha: {exc}")


def main() -> None:
    st.set_page_config(page_title="Oktto Pipeline", page_icon="📊", layout="centered")
    st.title("📊 Oktto Pipeline")
    st.caption("Extraia dados do seu CRM Oktto — baixe como CSV ou envie direto para o Google Sheets.")

    _how_it_works()

    st.divider()

    token = _token_input()

    defaults = _load_env_defaults()

    tab_csv, tab_sheets = st.tabs(["📥 Extrair CSV", "📤 Enviar para Google Sheets"])

    with tab_csv:
        _section_extract_csv(token, defaults)

    with tab_sheets:
        _section_send_to_sheets(token, defaults)


if __name__ == "__main__":
    main()
