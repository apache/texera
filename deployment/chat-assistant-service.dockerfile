################################################################################
# texera/deployment/chat-assistant-service.dockerfile
################################################################################
FROM python:3.12-slim

# ────────────────── 1. Copy source first so WORKDIR points at it ─────────────
COPY core/chat-assistant-service /core/chat-assistant-service

# ────────────────── 2. Set the working directory to the project root ─────────
WORKDIR /core/chat-assistant-service

# ────────────────── 3. Install dependencies  ─────────────────────────────────
RUN pip install --no-cache-dir -r requirements.txt

# ────────────────── 4. Ensure entrypoint is Unix-formatted & executable ───────
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]

# ────────────────── 5. Expose service port ───────────────────────────────────
EXPOSE 8001