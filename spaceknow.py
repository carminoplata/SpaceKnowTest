import json
import geojson
import logging
import logging.config
import os
import requests
import time

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from functools import wraps
from geojson import GeometryCollection
from json import JSONDecodeError
from requests.exceptions import ConnectionError
#from threading import Event, Timer



logging.config.fileConfig('logging.conf')
logger = logging.getLogger('SpaceKnow')
load_dotenv()

app = Flask(__name__)
app.config["DEBUG"] = True

def process(url, data='', token=''):
  
  headers = prepare_auth_header(token) if token else ''
  
  try:
    response = requests.post(url, data=data, headers=headers)
    if response.status_code >= 400:
        errors = response.json()
        if 'errorMessage' in errors:
          raise CommError(errors['errorMessage'], response.status_code)
        else:
          logger.error('Invalid error received from the server')
          message = 'SpaceKnow not available at the moment. Retry later!' \
            if response.status_code >= 500 else 'Invalid Request'
          raise CommError(message, response.status_code)
    return response.json()
  except (ConnectionError, requests.Timeout, requests.TooManyRedirects):
      logger.error("Impossible to connect at %s" % url)
      raise CommError('Impossible to connect at %s' % url, -1)
  except JSONDecodeError:
      logger.error("")
      raise CommError('Response is not a valid JSON', -1)

def authenticate():
    """ Get a Token valid for 10 hours
    """
    data = {"client_id": os.getenv('SPACEKNOW_CLIENT_ID'),
            "username": os.getenv('USERNAME'),
            "password": os.getenv('PASSWORD'),
            "connection": "Username-Password-Authentication",
            "grant_type": "password",
            "scope": "openid"}
    url = os.getenv('SPACEKNOW_AUTH0')

    try:
      authData = process(url, data)
      if 'id_token' not in authData:
        raise CommError('Token unavailable. Invalid credentials', 400)
      elif 'token_type'  not in authData or authData['token_type'] != 'bearer':
        raise CommError('Invalid Token. Expected Bearer Token!', 500)
      logger.debug('Token %s' % authData['id_token'])
      return authData['id_token']
    except CommError as e:
      logger.error("Error {}: {}".format(str(e.status_code), e.error))

def prepare_auth_header(token):
  """ Create Authorization Header for POST requests at SpaceKnowAPI
  """
  return {'Content-type': 'application/json',
          'Authorization': 'Bearer {}'.format(token)}

def getPermissions(token):
  """ Get a list of operations provided by SpaceKnowAPI available for the user
  """
  url = os.getenv('SK_USER_API') + "/info"
  try:
    jsonData = process(url, token=token)
    if 'permissions' not in jsonData:
      raise CommError('Invalid response', 500)
    else:
      return jsonData['permissions']
  except CommError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))

def createBrisbaneArea():
  try:
    with open(os.getenv('GEOJSON_FILE')) as fp:
      brisbaneArea = geojson.load(fp)
      if not brisbaneArea.is_valid:
        raise CommError('Invalid GeoJson file!', 400)
      return brisbaneArea
  except FileNotFoundError:
    logger.error("Error: file %s not found" % os.getenv('GEOJSON_FILE'))
    exit()

def prepare_searchReq(area):
  jsonExtent = GeometryCollection([area['geometry']])
  
  if not jsonExtent.is_valid:
    raise CommError("Invalid Extent Object", 400)
  data = {'provider':os.getenv('PROVIDER_GBDX'),
          'dataset': os.getenv('GBDX_IDAHO_DB'),
          'startDatetime': '2018-04-01 00:00:00',
          'endDatetime': '2018-04-30 23:59:59',
          'onlyDownloadable': True,
          'extent': jsonExtent}
  return json.dumps(data)

def searchImagery(permissions, token, extent):
  if os.getenv('IMG_AVAILABILITY') not in permissions:
    print("User has not got the rights for access at imagery dataset")
    return

  url = os.getenv('SK_IMAGE_API') + '/search/initiate'
  
  response = process(url, data=prepare_searchReq(extent), token=token)
  if 'pipelineId' not in response or 'nextTry' not in response or 'status' not in response:
    raise CommError('Invalid response', 500)
  
  if response['status'] == 'FAILED':
    raise CommError('Error during pipeline processing', 500)
  elif response['status'] == 'PROCESSING' or response['status'] == 'NEW':
    return response['nextTry'], response['pipelineId']
  #elif response['status'] == 'RESOLVED':
    # call retrieve
  else:
    raise CommError('Invalid status {}'.format(response['status']), 500)

def check_status(pipelineId, tryIn, token):
  def isReady(pipelineId, tryIn, token):
    url = os.getenv('SK_TASK_API')+'/get-status'
    response = process(url, data=pipelineId, token=token)
    if 'status' not in response or \
      (response['status']!='RESOLVED' and 'nextTry' not in response) :
      raise CommError(('Invalid response during checking the pipeline\'s status: %s', 
                      pipelineId), 500)
    if response['status'] == 'RESOLVED':
      return True
    elif response['status'] == 'FAILED':
      raise CommError('Error during pipeline processing', 500)
    else:
      tryIn = response['nextTry']
      return False
    
     
  #status = Event()
  time.sleep(tryIn)
  try:
    while not isReady(pipelineId, tryIn, token):
      print("Imagery is not available yet. Retry in %d"% tryIn)
      time.sleep(tryIn)
  except CommError as e:
     logger.error("Error %d during status checking at pipeline %s: %s" % 
                    (e.status_code, pipelineId, e.error))

def downloadImagery(pipelineId, token):
  """ Download imagery from pipelineId 
  """
  url = os.getenv('SK_IMAGE_API') + '/search/retrieve'
  response = process(url, data=pipelineId, token=token)
  if 'results' not in response or len(response['results'])==0:
    raise CommError('Any imagery found in the response', 500)
  return response['results']
  

class CommError(Exception):
  def __init__(self, error, status_code):
    self.error = error
    self.status_code = status_code

"""
class SpaceKnowManager:

  def __init__(self):
    logger.debug('Initialize SpaceKnowManager')
    

@app.errorhandler(CommError)
def handle_auth_error(ex):
  response = jsonify(ex.error)
  response.status_code = ex.status_code
  return response

@app.route('/')
def hello_world():
  try:
    authenticate()
  except CommError as e:
    app.logger.error(e.error['code'] + ': ' + e.error['description'])
  return 'Hello World!'
"""


if __name__ == "__main__":
  print("Authenticating at SpaceKnowAPI...")
  token = authenticate()
  if len(token) == 0:
    logger.error("Invalid token!")
    exit()
  print("Received token: %s" % token)
  permissions = getPermissions(token)
  if not permissions or len(permissions) == 0:
    logger.error("Impossible to check permission available for the users")
    exit()
  print("Selecting Brisbane Airport Area for the analysis...")
  area = createBrisbaneArea()
  try:
    print("Downloading imagery for Staff Parking Lot...")
    tryInSec, pipelineId =  searchImagery(permissions, token, area)
    print("Created Pipeline %s. Try to get results in %d seconds" % (pipelineId, tryInSec))
    pipelineJson = json.dumps({"pipelineId": pipelineId})
    check_status(pipelineJson, tryInSec, token)
    print("Imagery is ready! Downloading...")
    scenes = downloadImagery(pipelineJson, token)
    print("Downloaded %d scenes"% len(scenes))
  except CommError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))
    print("Error during the download check spaceknow.log for details")
    exit()
  #app.run()