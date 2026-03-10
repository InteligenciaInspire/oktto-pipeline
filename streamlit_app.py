import os
import sys

import streamlit as st


EXPECTED_PYTHON = (3, 12)
SUPPORTED_MIN_PYTHON = (3, 12)
SUPPORTED_MAX_PYTHON = (3, 14)


def _log_boot_diagnostics() -> None:
    print(
        "[boot] streamlit_app.py "
        f"python={sys.version.split()[0]} "
        f"cwd={os.getcwd()}",
        flush=True,
    )


def _validate_runtime() -> None:
    current = sys.version_info[:2]
    current_label = sys.version.split()[0]
    expected_label = ".".join(str(part) for part in EXPECTED_PYTHON)

    if not (SUPPORTED_MIN_PYTHON <= current < SUPPORTED_MAX_PYTHON):
        st.error("Runtime Python nao suportado para este deploy.")
        st.code(
            "Repositorio validado em Python 3.12.\n"
            f"Runtime detectado: {current_label}\n\n"
            "No Streamlit Community Cloud, recrie o app e selecione Python 3.12 em Advanced settings."
        )
        st.stop()

    if current != EXPECTED_PYTHON:
        st.warning(
            f"Runtime detectado: {current_label}. "
            f"Este repositorio e validado em Python {expected_label}."
        )


_log_boot_diagnostics()
_validate_runtime()

try:
    from src.ui.app import main
    main()
except Exception as e:
    st.error(f"Erro ao iniciar o app: {e}")
    import traceback
    st.code(traceback.format_exc())
