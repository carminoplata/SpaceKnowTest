import logging
import logging.config
import os
import requests

from dotenv import load_dotenv
from json import JSONDecodeError
from requests.exceptions import ConnectionError

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('SpaceKnow')
load_dotenv()

SK_IMAGE_API = os.getenv('SK_IMAGE_API')
SK_TASK_API = os.getenv('SK_TASK_API')
SK_KRAKEN_API = os.getenv('SK_KRAKEN_API')
SK_AUTH0 = os.getenv('SPACEKNOW_AUTH0')
SK_USER_API = os.getenv('SK_USER_API')
SK_CREDIT_API = os.getenv('SK_CREDIT_API')

def prepare_auth_header(token):
  """ Create Authorization Header for POST requests at SpaceKnowAPI
  """
  return {'Content-type': 'application/json',
          'Authorization': 'Bearer {}'.format(token)}

def buildPermission(prefix, provider, dataset):
  return prefix + '.' + provider + 'dataset'

def validateAccessRights(permissionsNeeds: list, userPermissions: list):
  for permission in permissionsNeeds:
    if permission not in userPermissions: 
      raise SpaceKnowError('User is unauthorized to perform the operation', 401)

def process(url, data='', token='', isGET=False):
  """ Sends a request at SpaceKnow API.
    Arguments:
    url -- SpaceKnow endpoint
    data -- json object to provide at the endpoint
    token -- user token to fill up Authorization field
  """
  headers = prepare_auth_header(token) if token else ''
  
  try:
    if not isGET:
      response = requests.post(url, data=data, headers=headers)
    else:
      response = requests.get(url, data=data, headers=headers)
    if response.status_code >= 400:
        errors = response.json()
        if 'errorMessage' in errors:
          raise SpaceKnowError(errors['errorMessage'], response.status_code)
        else:
          logger.error('Invalid error received from the server')
          message = 'SpaceKnow not available at the moment. Retry later!' \
            if response.status_code >= 500 else 'Invalid Request'
          raise SpaceKnowError(message, response.status_code)
    return response.json()
  except (ConnectionError, requests.Timeout, requests.TooManyRedirects):
      logger.error("Impossible to connect at %s" % url)
      raise SpaceKnowError('Impossible to connect at %s' % url, -1)
  except JSONDecodeError:
      logger.error("")
      raise SpaceKnowError('Response is not a valid JSON', -1)


def authenticate():
    """ Get a Token valid for 10 hours
    """
    data = {"client_id": os.getenv('SPACEKNOW_CLIENT_ID'),
            "username": os.getenv('USERNAME'),
            "password": os.getenv('PASSWORD'),
            "connection": "Username-Password-Authentication",
            "grant_type": "password",
            "scope": "openid"}
    try:
      authData = process(SK_AUTH0, data)
      if 'id_token' not in authData:
        raise SpaceKnowError('Token unavailable. Invalid credentials', 400)
      elif 'token_type'  not in authData or authData['token_type'] != 'bearer':
        raise SpaceKnowError('Invalid Token. Expected Bearer Token!', 500)
      logger.debug('Token %s' % authData['id_token'])
      return authData['id_token']
    except SpaceKnowError as e:
      logger.error("Error {}: {}".format(str(e.status_code), e.error))

def getPermissions(token):
  """ Get a list of operations provided by SpaceKnowAPI available for the user
  """
  url = SK_USER_API + "/info"
  try:
    jsonData = process(url, token=token)
    if 'permissions' not in jsonData:
      raise SpaceKnowError('Invalid response', 500)
    else:
      return jsonData['permissions']
  except SpaceKnowError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))

class SpaceKnowError(Exception):
  def __init__(self, error, status_code):
    self.error = error
    self.status_code = status_code