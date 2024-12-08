import streamlit as st
import encrypt
import yaml

# st.set_page_config(page_title="demo", initial_sidebar_state="expanded")
st.set_page_config(layout="wide", initial_sidebar_state="auto")
# Load encrypted information
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

if "query" not in st.session_state:
    st.session_state.query = ""






