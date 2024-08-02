FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 50051

# Use bash to source the setup.sh script before starting the server
CMD ["bash", "-c", "source /app/setup.sh && python recommendation_service/feed_rec_server.py"]