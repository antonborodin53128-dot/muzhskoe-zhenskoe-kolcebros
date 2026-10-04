# Точка входа для хостинга: `gunicorn app:app` (настройки сервера лежат в gunicorn.conf.py).
# Сама игра — в kolcebros_app.py, он же подключается в общий сборник как есть.
import os

from kolcebros_app import app, socketio  # noqa: F401

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), allow_unsafe_werkzeug=True)
