
## Setup

```
python -m venv venv
venv/Scripts/activate
pip install -r requirements.txt
```
or 
`pip install pandas, requests, psycopg2, sqlalchemy, pyodbc, python-dotenv`
## get daily data

```
python main.py
```

## Get avl data and push to API

```
python avl.py
```

## Flow

```mermaid

graph LR

A[Init 5 years data] -- Send through API --> B(Database)
B --> D{New Dataset}
A --> C(Daily new records dataset) --> D{New Dataset}
C --> B
B --> C