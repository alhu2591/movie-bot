#!/usr/bin/env bash

set -e  # وقف عند أول خطأ

# إنشاء بيئة افتراضية (اختياري بس منصوح فيه)
python3.11 -m venv .venv
source .venv/bin/activate

# تحديث pip وتنصيب الحزم
pip install --upgrade pip
pip install -r requirements.txt

# تثبيت متصفحات Playwright الخاصة بـ Python
python -m playwright install
