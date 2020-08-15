import json
import requests
import datetime

class Client:
    """ Client class handles reading and writing authentication codes for TDameritrade API
        member functions:
            load_credentials()
            save_credentials()
            new_credentials(refresh=False)
            """

    def __init__(self):
        self.load_credentials()
    

    def load_credentials(self, file_name='default.json'):
        with open("credentials/" + file_name, "r") as token_file:
            credentials = json.load(token_file)
            self.access_token = credentials["access_token"]
            self.refresh_token = credentials["refresh_token"]
            self.client_id = credentials["client_id"]


    def save_credentials(self, file_name='default.json'):
        credentials = {
                'access_token': self.access_token,
                'refresh_token': self.refresh_token,
                'client_id': self.client_id
                }

        with open("credentials/" + file_name, "w") as token_file:
            json.dump(credentials, token_file, indent=4)


    def new_credentials(self, refresh=False):
        """ Refresh parameter should be set to true sparingly, since refresh tokens
        expire after 90 days. un-necessary requests should be avoided"""

        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id
            }
        if (refresh):
            payload['access_type'] = 'offline'

        res = requests.post("https://api.tdameritrade.com/v1/oauth2/token", data=payload)
        if res.status_code == 200:
            data = res.json()

            self.access_token = data["access_token"]
            self.access_expire = datetime.datetime.now() + datetime.timedelta(seconds=data["expires_in"])
            if (refresh):
                self.refresh_token = data["refresh_token"]

class ResponseError(Exception):
    """ Raised when TD Ameritrade sends an error response
    """
    pass

class NoDataError(Exception):
    """ Raised when TD Ameritrade returns no data
    """
    pass