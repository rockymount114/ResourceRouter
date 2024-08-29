




#


```mermaid

graph LR

A[Init 5 years data] -- Send through API --> B(Database)
B --> D{New Dataset}
A --> C(Daily new records dataset) --> D{New Dataset}
C --> B
B --> C