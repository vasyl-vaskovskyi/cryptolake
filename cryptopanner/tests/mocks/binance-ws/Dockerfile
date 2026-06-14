FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY mock_ws.py fixture.jsonl ./
EXPOSE 9001
CMD ["python", "mock_ws.py"]
