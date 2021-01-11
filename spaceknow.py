import geojson
import json
import kraken
import logging
import logging.config
import os
import requests
import time
import utils
import sys

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from functools import wraps
from geojson import GeometryCollection
from json import JSONDecodeError
from kraken import KrakenManager
from pipeline import Pipeline
from requests.exceptions import ConnectionError
from utils import authenticate, getPermissions, process, SpaceKnowError, \
  validateAccessRights, buildPermission

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('Main')

def createBrisbaneArea(filename = ''):
  try:
    if not filename or len(filename) == 0:
      filename = os.getenv('GEOJSON_FILE')
    with open(filename) as fp:
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
  logger.info("Created Pipeline. Waiting for results...")
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
  logger.info("Created Pipeline. Waiting for results...")
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

def downloadCarImagery(scenes, token, permissions, extent):
  permissionsNeeds = [os.getenv('KRAKEN_RELEASE'),
                      buildPermission(os.getenv('IMAGERY_IMAGES'),
                                      os.getenv('PROVIDER_GBDX'),
                                      os.getenv('GBDX_IDAHO_DB'))]
  validateAccessRights(permissionsNeeds, permissions)
  return kraken.downloadMaps('cars', scenes, token, extent)

def downloadImagery(scenes, token, permissions, extent):
  permissionsNeeds = [os.getenv('KRAKEN_RELEASE'),
                      buildPermission(os.getenv('IMAGERY_IMAGES'),
                                      os.getenv('PROVIDER_GBDX'),
                                      os.getenv('GBDX_IDAHO_DB'))]
  validateAccessRights(permissionsNeeds, permissions)
  return kraken.downloadMaps('imagery', scenes, token, extent)

def getConfigurations(user='', password=''):
  if not user or len(user)==0:
    user = os.getenv('USERNAME')
  if not password or len(password)==0:
    password = os.getenv('PASSWORD')
  
  token = authenticate(user, password)
  if not token or  len(token) == 0:
    logger.error("Invalid token!")
    exit()
  logger.info("Congratulations, you are in SpaceKnow!")
  return token

def runCarDetections(user='', password='', filename=''):
  logger.info("Authenticating at SpaceKnowAPI...")
  token = getConfigurations(user, password)
  permissions = getPermissions(token)
  if not permissions or len(permissions) == 0:
    logger.error("Impossible to check permission available for the users")
    exit()
  logger.info("Selecting Brisbane Airport Area for the analysis...")
  area = createBrisbaneArea(filename)
  try:
    logger.info("Downloading imagery for Staff Parking Lot...")
    scenes =  searchImagery(permissions, token, area)
    logger.info("Downloaded %d scenes"% len(scenes))
    logger.info("Making cost analysis on all scenes...")
    costAnalysis = evaluatesCosts(scenes, area, permissions, token)
    logger.info("Brisbane Area total size: %.4f km2" % costAnalysis['ingestedKm2'])
    logger.info("Brisbane Area size to analyze: %.4f km2" % costAnalysis['analyzedKm2'])
    logger.info("Brisbane Area allocated size: %.4f km2" % costAnalysis['allocatedKm2'])
    logger.info("Credits required: %.4f" % costAnalysis['allocatedCredits'])
    userCredits = getCreditsAvailable(token, permissions)
    if userCredits < costAnalysis['allocatedCredits'] :
      logger.info("Impossible to make analysis!\n The user %s does not have enough"
            " credits.\n Available credits: %.2f" % (os.getenv('USERNAME'), 
            userCredits))
      exit()
   
    logger.info("My credits: %.2f" % userCredits)
    logger.info("Downloading Imagery Maps...")
    carMaps = downloadCarImagery(scenes, token, permissions, area)
    logger.info("Downloaded %d imageries" % len(carMaps))
    krakenManager = KrakenManager()
    logger.info("Detecting cars inside imageries...")
    results = krakenManager.process(carMaps, 'CAR_DETECTION')
    if len(results) == 0:
      logger.info("No cars was found in this area!")
      exit()
    total = 0
    for mapId, pair in results.items():
      cars = pair[0]
      tiles = pair[1]
      total += cars
      logger.info("Found %d cars for mapId %s"% (cars, mapId[-10:]))
      logger.info("Building PNG file for mapId %s" % mapId[-10:])
      krakenManager.build_image(mapId, tiles, 'BUILD_CARS_PNG')
      logger.info("Created image %s_detection.png"%mapId[-10:])

    logger.info("Found %d cars in total" % total)
    
    logger.info("Downloading all analyzed imageries...")
    imageryMaps = downloadImagery(scenes, token, permissions, area)
    logger.info("Downloaded %d imageries" % len(imageryMaps))
    logger.info("Building satellite images...")
    krakenManager.process(imageryMaps, 'BUILD_PNG')
    logger.info("All satellite images are ready!")
    logger.info("Summary: \n Cars in the area: %d \n All satellite and tiles images" 
      " are in output folder!" % total)
  except SpaceKnowError as e:
    logger.error("Error {}: {}".format(str(e.status_code), e.error))
    logger.info("Error during the processing check spaceknow.log for details")
    exit()

if __name__ == "__main__":
  
  if len(sys.argv) == 1:
    runCarDetections()
  elif len(sys.argv) < 3:
    print("Error: Invalid Arguments! Run at least as spaceknow.py <username> "
          "<password>")
  else:
    user = sys.argv[1]
    password = sys.argv[2]
    filename = sys.argv[3] if len(sys.argv)==4 else ''
    runCarDetections(user, password, filename)