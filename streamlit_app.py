import streamlit as st

try:
    from src.ui.app import main
    main()
except Exception as e:
    st.error(f"Erro ao iniciar o app: {e}")
    import traceback
    st.code(traceback.format_exc())
