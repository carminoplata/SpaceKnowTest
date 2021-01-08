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
from pipeline import Pipeline
from requests.exceptions import ConnectionError
from utils import authenticate, getPermissions, process, logger, SpaceKnowError, \
  validateAccessRights

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
          'startDatetime': '2018-04-01 00:00:00',
          'endDatetime': '2018-04-30 23:59:59',
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
  validateAccessRights(os.getenv('IMG_AVAILABILITY'), permissions)
  url = utils.SK_IMAGE_API + '/search'
  pipeline = Pipeline(url, token, prepare_searchReq(extent))
  pipeline.start()
  print("Created Pipeline. Waiting for results...")
  response = pipeline.join()
  if not response or 'results' not in response or len(response['results'])==0:
    raise SpaceKnowError('Any imagery found in the response', 500)
  return response['results']
  
def evaluatesCosts(scenes, extent, permissions, token):
  validateAccessRights(os.getenv('KRAKEN_DRY_RUN'), permissions)
  data = createEvaluationRequest(scenes, extent)
  url = utils.SK_KRAKEN_API + '/dry-run'
  pipeline = Pipeline(url, token, data)
  pipeline.start()
  print("Created Pipeline. Waiting for results...")
  analysis = pipeline.join()
  if not analysis or 'allocatedCredits' not in analysis:
    raise SpaceKnowError('Cost analysis is not available', 503)
  return analysis
    

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
    scenes =  searchImagery(permissions, token, area)
    print("Downloaded %d scenes"% len(scenes))
    print("Making cost analysis on all scenes...")
    costAnalysis = evaluatesCosts(scenes, area, permissions, token)
    print("Brisbane Area total size: %.4f km2" % costAnalysis['ingestedKm2'])
    print("Brisbane Area size to analyze: %.4f km2" % costAnalysis['analyzedKm2'])
    print("Brisbane Area allocated size: %.4f km2" % costAnalysis['allocatedKm2'])
    print("Credits required: %.4f" % costAnalysis['allocatedCredits'])
  except SpaceKnowError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))
    print("Error during the processing check spaceknow.log for details")
    exit()
  #app.run()