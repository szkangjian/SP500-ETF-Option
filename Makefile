PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python

.PHONY: venv install test dashboard

venv:
	$(PYTHON) -m venv $(VENV)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

install: venv

test:
	$(VENV_PYTHON) -m unittest discover -s tests -p 'test_*.py'

dashboard:
	$(VENV_PYTHON) screening_dashboard.py --port 8765
