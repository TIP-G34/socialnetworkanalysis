#!/bin/sh
pip freeze > requirements.txt
git add .
git commit -m "$1"
git push origin main