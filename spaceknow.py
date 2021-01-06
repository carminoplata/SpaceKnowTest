import json
import logging
import logging.config
import os
import requests

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from functools import wraps
from requests.exceptions import ConnectionError
from json import JSONDecodeError


logging.config.fileConfig('logging.conf')
logger = logging.getLogger('SpaceKnow')
load_dotenv()

app = Flask(__name__)
app.config["DEBUG"] = True

def authenticate():
    """ Get a Token valid for 10 hours
    """
    data = {"client_id": os.getenv('SPACEKNOW_CLIENT_ID'),
            "username": os.getenv('USERNAME'),
            "password": os.getenv('PASSWORD'),
            "connection": "Username-Password-Authentication",
            "grant_type": "password",
            "scope": "openid"}
    url = os.getenv('SPACEKNOW_AUTH0_DOMAIN')
    try:
      response = requests.post(url, data)
      
      if response.status_code >= 500:
        logger.error('SpaceKnow is not available at the moment. Retry later!')
      elif response.status_code >= 400:
        logger.error('Authorization request invalid!')
      else:
        authData = response.json()
        if 'id_token' not in authData:
          logger.error('Token unavailable. Invalid credentials')
        elif 'token_type'  not in authData or authData['token_type'] != 'bearer':
          logger.error('Invalid Token. Expected Bearer Token!')
        logger.debug('Token %s' % authData['id_token'])
        return authData['id_token']
    except (ConnectionError, requests.Timeout, requests.TooManyRedirects):
      logger.error("Impossible to connect at %s" % url)
    except JSONDecodeError:
      logger.error("Response is not a valid JSON")


class AuthError(Exception):
  def __init__(self, error, status_code):
    self.error = error
    self.status_code = status_code

class SpaceKnowManager:

  def __init__(self):
    logger.debug('Initialize SpaceKnowManager')
    

@app.errorhandler(AuthError)
def handle_auth_error(ex):
  response = jsonify(ex.error)
  response.status_code = ex.status_code
  return response


@app.route('/')
def hello_world():
  try:
    authenticate()
  except AuthError as e:
    app.logger.error(e.error['code'] + ': ' + e.error['description'])
  return 'Hello World!'

if __name__ == "__main__":
  authenticate()
  app.run()