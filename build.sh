#!/usr/bin/env bash
pip install -r requirements.txt --no-cache-dir --force-reinstall
npx playwright install --with-deps
