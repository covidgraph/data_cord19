# CORD-19 Data loader

This python script helps to transform the data set from the [COVID-19 Open Research Dataset Challenge](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge/data)
into a neo4j graph

Maintainer: [Tim](https://github.com/motey)

Version: 0.0.1

Python-version: Python3

# Usage

Enter your neo4j connection string in `dataloader/env/DEFAULT.env` into the variable `CONFIGS_NEO4J_CON`

(or create a new .env file with you environment name. For more details have a look at the [Configs](https://git.connect.dzd-ev.de/dzdtools/pythonmodules/tree/master/Configs) module)

```
CONFIGS_NEO4J_CON='bolt://localhost:7687'
```

Install the requirements with

`pip3 install -r dataloader/requirement.txt`

run the main.py

`python3 main.py`

# Data

At the moment only the json files in the dataset will be imported

Have a look at dataloader/config.py -> DATA_DIRS . There is a list of all directories in the dataset which will be taken into account

# ToDo

- Create a docker image
