import pandas as pd
import requests

# df = pd.read_csv('test.csv')

def upload_to_api(csv_file):
    url = "https://hooks.prismatic.io/trigger/SW5zdGFuY2U6Mzc3MjBiNjctOGI5OC00NWI2LWExODktZTNjOGJjOWQzZjFh"
    headers = {
        "Content-Type": "text/csv",
        "Api-Key": '6880382b49db5dbf70a314294bb3eab9'
    }
    with open(csv_file, 'rb') as f:
        response = requests.post(url, headers=headers, data=f)
    return response


if __name__ == "__main__":
    response = upload_to_api('test.csv')
    print(f'Daily data upload response: {response.status_code} - {response.text}')