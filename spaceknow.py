import json
import geojson
import logging
import logging.config
import os
import requests
import time
import utils

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from functools import wraps
from geojson import GeometryCollection
from json import JSONDecodeError
from kraken import KrakenConsumer, KrakenProducer, MapsQueue
from pipeline import Pipeline
from requests.exceptions import ConnectionError
from utils import authenticate, getPermissions, process, logger, SpaceKnowError, \
  validateAccessRights, buildPermission

def createBrisbaneArea():
  try:
    with open(os.getenv('GEOJSON_FILE')) as fp:
      geoObj = geojson.load(fp)
      if not geoObj.is_valid:
        raise SpaceKnowError('Invalid GeoJson file!', 400)
      brisbaneArea = GeometryCollection([geoObj['geometry']])
      if not brisbaneArea.is_valid:
        raise SpaceKnowError("Invalid GeoJson Object", 400)
      return brisbaneArea
  except FileNotFoundError:
    logger.error("Error: file %s not found" % os.getenv('GEOJSON_FILE'))
    exit()

def prepare_searchReq(area):
  data = {'provider':os.getenv('PROVIDER_GBDX'),
          'dataset': os.getenv('GBDX_IDAHO_DB'),
          'startDatetime': '2018-01-01 00:00:00',
          'endDatetime': '2018-01-31 23:59:59',
          'onlyDownloadable': True,
          'extent': area}
  return json.dumps(data)

def createEvaluationRequest(scenes, extent):
  scenesID = [{ 'sceneId': s['sceneId'] for s in scenes }]
  mapTypes = ['cars']
  dryRunObj = {'scenes': scenesID,
               'mapTypes': mapTypes}
  dryRunsList = [dryRunObj]
  evRequest = {'dryRuns': dryRunsList,
             'extent': extent}
  return json.dumps(evRequest)

def searchImagery(permissions, token, extent):
  validateAccessRights([os.getenv('IMG_AVAILABILITY')], permissions)
  url = utils.SK_IMAGE_API + '/search'
  pipeline = Pipeline(url, token, prepare_searchReq(extent))
  pipeline.start()
  print("Created Pipeline. Waiting for results...")
  response = pipeline.join()
  if not response or 'results' not in response or len(response['results'])==0:
    raise SpaceKnowError('Any imagery found in the response', 500)
  return response['results']
  
def evaluatesCosts(scenes, extent, permissions, token):
  validateAccessRights([os.getenv('KRAKEN_DRY_RUN')], permissions)
  data = createEvaluationRequest(scenes, extent)
  url = utils.SK_KRAKEN_API + '/dry-run'
  pipeline = Pipeline(url, token, data)
  pipeline.start()
  print("Created Pipeline. Waiting for results...")
  analysis = pipeline.join()
  if not analysis or 'allocatedCredits' not in analysis:
    raise SpaceKnowError('Cost analysis is not available', 503)
  return analysis

def getCreditsAvailable(token, permissions):
  validateAccessRights([os.getenv('CREDITS_AVAILABLE')], permissions)
  url = utils.SK_CREDIT_API + '/get-remaining-credit'
  response = process(url=url, token=token)
  if 'remainingCredit' not in response:
    raise SpaceKnowError('Invalid response from server', 500)
  return response['remainingCredit']

def getImagery(scenes, token, permission, extent):
  permissionsNeeds = [os.getenv('KRAKEN_RELEASE'),
                      buildPermission(os.getenv('ALGORITHM_CAR_DETECTION'),
                                      os.getenv('PROVIDER_GBDX'),
                                      os.getenv('GBDX_IDAHO_DB'))]
  validateAccessRights(permissionsNeeds, permissions)
  imageriesQueue = MapsQueue(len(scenes))
  producer = KrakenProducer('imagery', scenes, extent, imageriesQueue, token)
  consumer = KrakenConsumer(token, imageriesQueue, resource='truecolor.png',)
  producer.run()
  print("Cars detection algortihm started")
  producer.join()
  consumer.run('imagery.png')

def executeAnalysis(scenes, token, permissions, extent):
  permissionsNeeds = [os.getenv('KRAKEN_RELEASE'),
                      buildPermission(os.getenv('ALGORITHM_CAR_DETECTION'),
                                      os.getenv('PROVIDER_GBDX'),
                                      os.getenv('GBDX_IDAHO_DB'))]
  validateAccessRights(permissionsNeeds, permissions)
  mapsQueue = MapsQueue(len(scenes))
  producer = KrakenProducer('cars', scenes, extent, mapsQueue, token)
  consumer = KrakenConsumer(token, mapsQueue, resource='cars.png')
  producer.run()
  print("Cars detection algortihm started")
  producer.join()
  consumer.run('cars.png')

"""
class SpaceKnowManager:

  def __init__(self):
    logger.debug('Initialize SpaceKnowManager')
    

@app.errorhandler(SpaceKnowError)
def handle_auth_error(ex):
  response = jsonify(ex.error)
  response.status_code = ex.status_code
  return response

@app.route('/')
def hello_world():
  try:
    authenticate()
  except SpaceKnowError as e:
    app.logger.error(e.error['code'] + ': ' + e.error['description'])
  return 'Hello World!'
"""


if __name__ == "__main__":
  print("Authenticating at SpaceKnowAPI...")
  token = authenticate()
  if not token or  len(token) == 0:
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
    scenes =  searchImagery(permissions, token, area)
    print("Downloaded %d scenes"% len(scenes))
    print("Making cost analysis on all scenes...")
    costAnalysis = evaluatesCosts(scenes, area, permissions, token)
    print("Brisbane Area total size: %.4f km2" % costAnalysis['ingestedKm2'])
    print("Brisbane Area size to analyze: %.4f km2" % costAnalysis['analyzedKm2'])
    print("Brisbane Area allocated size: %.4f km2" % costAnalysis['allocatedKm2'])
    print("Credits required: %.4f" % costAnalysis['allocatedCredits'])
    userCredits = getCreditsAvailable(token, permissions)
    if userCredits < costAnalysis['allocatedCredits'] :
      print("Impossible to make analysis!\n The user %s does not have enough"
            " credits.\n Available credits: %.2f" % (os.getenv('USERNAME'), 
            userCredits))
      exit()
   
    print("My credits: %.2f" % userCredits)
    print("Get Imagery")
    getImagery(scenes, token, permissions, area)

    executeAnalysis(scenes, token, permissions, area)
    #print("Detected %d cars in Brisbane Area" % len(tiles))
  except SpaceKnowError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))
    print("Error during the processing check spaceknow.log for details")
    exit()
  #app.run()