#!/usr/bin/env bash
# What the interop test needs: a redis, and Celery 5.5 in a virtualenv of its own.
# The venv lives under $HOME rather than /tmp, because /tmp in WSL is cleared
# between sessions and the test should not have to reinstall for that.
set -eu
VENV="${VENV:-$HOME/.celery-venv}"

docker rm -f ct-redis >/dev/null 2>&1 || true
docker run -d --rm --name ct-redis -p 6379:6379 redis:7 >/dev/null
for _ in $(seq 1 20); do
  docker exec ct-redis redis-cli ping 2>/dev/null | grep -q PONG && break
  sleep 1
done

if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
fi
"$VENV/bin/pip" install -q "celery[redis]==5.5.3"

"$VENV/bin/python" - <<'PY'
import celery, kombu
print(f"celery {celery.__version__}, kombu {kombu.__version__}")
PY
