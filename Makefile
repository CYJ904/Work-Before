# design for linux
SHELL := /usr/bin/zsh
run:
	source ~/.zshrc && conda activate 431w && streamlit run app.py --server.address 0.0.0.0 &

stop:
	pkill -f streamlit 
