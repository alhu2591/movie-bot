#!/usr/bin/env bash
pip install -r requirements.txt --no-cache-dir --force-reinstall
# لا يوجد npx playwright install --with-deps هنا لأننا نستخدم Scrapy بدلاً من Playwright
