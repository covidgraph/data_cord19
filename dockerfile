FROM python:3.8

RUN mkdir -p /app/dataset
RUN mkdir -p /app/dataloader
WORKDIR /app/dataloader


COPY requirement.txt ./
RUN pip install --no-cache-dir -r requirement.txt
COPY dataloader .

CMD [ "python3", "./main.py" ]