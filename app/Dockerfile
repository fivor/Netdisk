FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -i https://mirrors.cloud.tencent.com/pypi/simple -r requirements.txt

COPY . .

EXPOSE 8100
# --proxy-headers + --forwarded-allow-ips：经 cloudflared/反代转发时信任 X-Forwarded-For，
# 让 request.client.host 拿到真实客户端 IP（下载计数去重、本机直连判定才不会被反代 IP 一刀切）。
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8100", \
     "--proxy-headers", "--forwarded-allow-ips", "*"]
