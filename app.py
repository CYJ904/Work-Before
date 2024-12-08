import streamlit as st
import encrypt


pages = [
        st.Page("table.py", default=True, title="table"),
        st.Page("display.py", default=False, title="display")
        ]

if "secret" not in st.session_state:
    password = st.text_input("Please input password for encryption", value=None, type="password")
    try:
        st.session_state.secret = encrypt.decrypt_yaml('secret.yaml.enc', 'secret.yaml.salt', password)
        st.rerun()
    except FileNotFoundError as e:
        st.error("Encryption file or salt file not fond. Please check your file paths or regenerate it.")
        st.exception(e)
        pass
    except ValueError as e:
        st.error("Decryption failed. The password might be incorrect or the files are corrupted.")
        st.exception(e)
    except AttributeError as e:
        st.stop()
    except Exception as e:
        st.error("An unexpected error occurred during decryption.")
        st.exception(e)

pg = st.navigation(pages, expanded=False)
pg.run()
