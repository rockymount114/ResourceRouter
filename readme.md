
## Setup

```
python -m venv venv
venv/Scripts/activate
pip install -r requirements.txt
```
## get daily data

```
python main.py
```

## Flow

```mermaid

graph LR

A[Init 5 years data] -- Send through API --> B(Database)
B --> D{New Dataset}
A --> C(Daily new records dataset) --> D{New Dataset}
C --> B
B --> C