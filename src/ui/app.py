from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st
from dotenv import dotenv_values, set_key
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient, SheetsClientOAuth
from src.config import OkttoSettings, SheetsSettings
from src.extract.additional_fields import fetch_additional_fields
from src.extract.funnels import fetch_funnels
from src.extract.leads import fetch_leads
from src.extract.sales import fetch_sales
from src.extract.teams import fetch_teams
from src.extract.users import fetch_users
from src.main import JOBS, run_job, run_job_with_clients
from src.transform.normalize_leads import normalize_leads
from src.transform.normalize_sales import normalize_sales


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def _is_streamlit_cloud() -> bool:
    try:
        return bool(st.secrets.get("APP_PUBLIC_MODE"))
    except Exception:
        return False


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
    is_cloud = _is_streamlit_cloud()
    values = dotenv_values(ENV_PATH)

    def _val(key: str, fallback: str = "") -> str:
        if is_cloud:
            return _secret(key, str(values.get(key, fallback)))
        return str(values.get(key, fallback))

    return {
        "OKTTO_API_BASE_URL": _val("OKTTO_API_BASE_URL", "https://api.oktto.com.br/v1"),
        "OKTTO_API_TOKEN": _val("OKTTO_API_TOKEN"),
        "GOOGLE_SHEETS_SPREADSHEET_ID": _val("GOOGLE_SHEETS_SPREADSHEET_ID"),
        "GOOGLE_SHEETS_CREDENTIALS_JSON": _val("GOOGLE_SHEETS_CREDENTIALS_JSON"),
        "GOOGLE_OAUTH_CLIENT_ID": _val("GOOGLE_OAUTH_CLIENT_ID"),
        "GOOGLE_OAUTH_CLIENT_SECRET": _val("GOOGLE_OAUTH_CLIENT_SECRET"),
        "GOOGLE_OAUTH_REDIRECT_URI": _val("GOOGLE_OAUTH_REDIRECT_URI"),
        "LOG_LEVEL": _val("LOG_LEVEL", "INFO"),
    }


def _save_env(updates: dict[str, str]) -> None:
    if _is_streamlit_cloud():
        st.warning("No Streamlit Cloud, configuracoes devem ser definidas em Settings > Secrets.")
        return
    ENV_PATH.touch(exist_ok=True)
    for key, value in updates.items():
        set_key(str(ENV_PATH), key, value)
        os.environ[key] = value


def _test_oktto_connection(base_url: str, token: str) -> tuple[bool, str]:
    if not token:
        return False, "Informe o token da Oktto."

    try:
        client = OkttoClient(
            OkttoSettings(
                base_url=base_url,
                token=token,
            )
        )
        payload = client.get("/permissions")
        count = len(payload) if isinstance(payload, list) else len(payload.keys()) if isinstance(payload, dict) else 0
        return True, f"Conexao OK. Resposta recebida em /permissions (itens: {count})."
    except Exception as exc:
        return False, f"Falha ao conectar na Oktto: {exc}"


def _test_sheets_connection(spreadsheet_id: str, credentials_json: str) -> tuple[bool, str]:
    if not spreadsheet_id or not credentials_json:
        return False, "Informe spreadsheet_id e caminho do JSON de credenciais."

    if not Path(credentials_json).exists():
        return False, "Arquivo de credenciais nao encontrado no caminho informado."

    try:
        client = SheetsClient(
            SheetsSettings(
                spreadsheet_id=spreadsheet_id,
                credentials_json=credentials_json,
            )
        )
        worksheet_count = len(client.spreadsheet.worksheets())
        return True, f"Conexao OK com Google Sheets. Abas encontradas: {worksheet_count}."
    except Exception as exc:
        return False, f"Falha ao conectar no Google Sheets: {exc}"


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


def _admin_panel(defaults: dict[str, str]) -> None:
    with st.form("config_form"):
        st.subheader("Configuracao")
        oktto_base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"])
        oktto_token = st.text_input("Oktto Token", value=defaults["OKTTO_API_TOKEN"], type="password")
        spreadsheet_id = st.text_input("Google Spreadsheet ID", value=defaults["GOOGLE_SHEETS_SPREADSHEET_ID"])
        credentials_json = st.text_input(
            "Caminho do JSON de credenciais",
            value=defaults["GOOGLE_SHEETS_CREDENTIALS_JSON"],
            help="Exemplo: credentials/google-service-account.json",
        )
        log_options = ["DEBUG", "INFO", "WARNING", "ERROR"]
        default_log_index = log_options.index(defaults["LOG_LEVEL"]) if defaults["LOG_LEVEL"] in log_options else 1
        log_level = st.selectbox("Log level", options=log_options, index=default_log_index)

        save = st.form_submit_button("Salvar configuracoes")
        if save:
            _save_env(
                {
                    "OKTTO_API_BASE_URL": oktto_base_url.strip(),
                    "OKTTO_API_TOKEN": oktto_token.strip(),
                    "GOOGLE_SHEETS_SPREADSHEET_ID": spreadsheet_id.strip(),
                    "GOOGLE_SHEETS_CREDENTIALS_JSON": credentials_json.strip(),
                    "LOG_LEVEL": log_level,
                }
            )
            st.success("Configuracoes salvas no .env")

    st.subheader("Testes de conexao")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Testar Oktto", use_container_width=True):
            ok, message = _test_oktto_connection(oktto_base_url.strip(), oktto_token.strip())
            if ok:
                st.success(message)
            else:
                st.error(message)

    with col2:
        if st.button("Testar Google Sheets", use_container_width=True):
            ok, message = _test_sheets_connection(spreadsheet_id.strip(), credentials_json.strip())
            if ok:
                st.success(message)
            else:
                st.error(message)

    st.subheader("Executar jobs")
    selected_job = st.selectbox("Escolha o job", options=list(JOBS.keys()), index=0)

    if st.button("Executar job", type="primary", use_container_width=True):
        try:
            run_job(selected_job)
            st.success(f"Job {selected_job} executado com sucesso.")
        except Exception as exc:
            st.error(f"Falha ao executar job {selected_job}: {exc}")


def _public_panel(defaults: dict[str, str]) -> None:
    st.subheader("Extracao publica (token por usuario)")
    st.info(
        "Cada usuario informa o proprio token da Oktto e pode extrair CSV ou enviar para o proprio Google Sheets via login Google. "
        "Nenhum token da Oktto e salvo no servidor."
    )

    _oauth_login_panel(defaults)

    with st.form("public_sheets_form"):
        st.markdown("#### Enviar para Google Sheets")
        oktto_base_url_sheets = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"], key="public_o_base")
        oktto_token_sheets = st.text_input("Seu token Oktto", type="password", key="public_o_token")
        spreadsheet_id = st.text_input("Spreadsheet ID de destino", key="public_sheet_id")
        selected_job = st.selectbox("Job", options=list(JOBS.keys()), index=0, key="public_job")
        send = st.form_submit_button("Executar e enviar para Sheets")

    if send:
        if not oktto_token_sheets.strip():
            st.error("Informe seu token Oktto.")
        elif not spreadsheet_id.strip():
            st.error("Informe o Spreadsheet ID.")
        else:
            try:
                with st.spinner("Executando job e escrevendo no Sheets..."):
                    _run_public_job_to_user_sheets(
                        job_name=selected_job,
                        oktto_base_url=oktto_base_url_sheets.strip(),
                        oktto_token=oktto_token_sheets.strip(),
                        spreadsheet_id=spreadsheet_id.strip(),
                    )
                st.success(f"Job {selected_job} executado e enviado para o Sheets informado.")
            except Exception as exc:
                st.error(f"Falha ao enviar para Sheets: {exc}")

    st.divider()

    with st.form("public_extract_form"):
        base_url = st.text_input("Oktto Base URL", value=defaults["OKTTO_API_BASE_URL"])
        token = st.text_input("Seu token Oktto", type="password")
        dataset = st.selectbox("Dataset", options=list(DATASET_EXTRACTORS.keys()), index=0)
        page_size = st.number_input("Itens por pagina", min_value=10, max_value=500, value=100, step=10)
        submit = st.form_submit_button("Extrair")

    if submit:
        if not token.strip():
            st.error("Informe o token da Oktto para extrair.")
            return
        try:
            with st.spinner("Extraindo dados..."):
                df = _public_extract(dataset, base_url.strip(), token.strip(), int(page_size))

            st.success(f"Extracao concluida. Linhas: {len(df)}")
            st.dataframe(df.head(100), use_container_width=True)

            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Baixar CSV",
                data=csv_data,
                file_name=f"oktto_{dataset}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except Exception as exc:
            st.error(f"Falha na extracao: {exc}")


def main() -> None:
    st.set_page_config(page_title="Oktto Pipeline", layout="centered")
    st.title("Oktto Pipeline - Painel Simples")
    st.caption("Modo admin para pipeline completo e modo publico para extracao por token individual.")

    is_public = _secret("APP_PUBLIC_MODE", os.getenv("APP_PUBLIC_MODE", "false")).lower() == "true"
    defaults = _load_env_defaults()

    if is_public:
        _public_panel(defaults)
    else:
        tab_admin, tab_public = st.tabs(["Admin", "Publico"])
        with tab_admin:
            _admin_panel(defaults)
        with tab_public:
            _public_panel(defaults)


if __name__ == "__main__":
    main()
