FROM python:3.6

WORKDIR /usr/src/dataloader

COPY requirement.txt ./
RUN pip install --no-cache-dir -r requirement.txt
COPY dataloader .

CMD [ "python3", "./main.py" ]