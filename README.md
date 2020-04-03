# CORD-19 Data loader

This python script helps to transform the data set from the [COVID-19 Open Research Dataset Challenge](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge/data)
into a neo4j graph

Maintainer: [Tim](https://github.com/motey)

Version: 0.0.1

Python-version: Python3

ThanksTo: https://pypi.org/project/cord-19-tools/

# Usage

## Docker

**Build**

From the root directorie of this repo run:

`docker build -t cord-19-data-loader .`

**Run**

`docker run -it --rm --name cord-19-data-loader -e CONFIGS_NEO4J_CON='bolt://myuser:mypasswd@myneo4jhostname:7687' cord-19-data-loader`

`docker run -it --rm --cord-19-data-loader -e CONFIGS_NEO4J_CON='bolt://neo4j:test@${HOSTNAME}:7687' cord-19-data-loader`

## Local

Copy `dataloader/env/DEFAULT.env` to `dataloader/env/DEVELOPMENT.env`:

`cp dataloader/env/DEFAULT.env dataloader/env/DEVELOPMENT.env`

Enter your neo4j connection string at `dataloader/env/DEVELOPMENT.env` into the variable `CONFIGS_NEO4J_CON`:

```env
CONFIGS_NEO4J_CON='bolt://myuser:mypasswd@localhost:7687'
```

Install the requirements with

`pip3 install -r dataloader/requirement.txt`

run the main.py

`python3 main.py`

# Data

Have a look at dataloader/config.py -> DATA_DIRS . There is a list of all directories in the dataset which will be taken into account

# ToDo

- Create a docker image
