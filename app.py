import streamlit as st
import encrypt
import utils
import yaml


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
        st.stop()
        pass
    except ValueError as e:
        st.error("Decryption failed. The password might be incorrect or the files are corrupted.")
        st.exception(e)
        st.stop()
    except AttributeError as e:
        st.stop()
    except Exception as e:
        st.error("An unexpected error occurred during decryption.")
        st.exception(e)
        st.stop()

if "stack" not in st.session_state:
    st.session_state.stack = []

if "connector" not in st.session_state:
    secret = st.session_state.secret
    st.session_state.connector = utils.NewConnector(user=secret['user_name'], password = secret['user_password'], host=secret['host'], port=secret['port'], database=secret['database'])
    st.session_state.connector.start_transaction()

if "query" not in st.session_state:
    st.session_state.query = {}
    st.session_state.query['body'] = ""
    st.session_state.query['value'] = tuple()

if "result" not in st.session_state:
    st.session_state.result = ""

if "display" not in st.session_state:
    st.session_state.display = []


if "data_changed" not in st.session_state:
    st.session_state.data_changed = False


if "map" not in st.session_state:
    with open('config.yaml', 'r') as file:
        st.session_state.map = yaml.safe_load(file)

pg = st.navigation(pages, expanded=False)
pg.run()
