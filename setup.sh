set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq --no-install-recommends redis-server python3-venv
systemctl enable --now redis-server
python3 -m venv /root/v
/root/v/bin/pip install -q celery dramatiq rq huey taskiq taskiq-redis arq redis packaging
/root/v/bin/pip freeze > /root/exp/versions.txt
redis-server --version >> /root/exp/versions.txt
