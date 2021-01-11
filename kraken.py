import concurrent.futures
import json
import os
import requests
import utils

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from os import path
from PIL import Image
from pipeline import Pipeline
from queue import Queue
from threading import Thread
from utils import SpaceKnowError, process, buildURL, spaceKnowLogger

KRAKEN_MAPS = {'imagery': ['truecolor.png', 
                           'imagery.ski',
                           'analysis.geotiff',
                           'visualization.geotiff',
                           'metadata.json'],
               'cars': ['cars.png',
                        'trucks.png',
                        'segmentation.ski',
                        'analysis.geotiff',
                        'visualization.geotiff',
                        'detections.geojson',
                        'metadata.json']}

KRAKEN_OPERATIONS = ['CAR_DETECTION', 'BUILD_PNG', 'BUILD_CARS_PNG']


def validateMap(mapType):
  if mapType not in KRAKEN_MAPS:
    raise SpaceKnowError('Unknown map type', 400)

def validateResource(mapType, resource):
  validateMap(mapType)
  if resource not in KRAKEN_MAPS[mapType]:
    raise SpaceKnowError('Unknown resource for mapType %s'% mapType, 400)

def validateOperations(operation):
  if operation not in KRAKEN_OPERATIONS:
    raise SpaceKnowError('Unknown operation', 404)

def downloadMaps(mapType, scenes, token, extent):
  validateMap(mapType)
  maps = []
  spaceKnowLogger.info('Downloading maps for %s from KRAKEN API...' % mapType)
  with ThreadPoolExecutor(max_workers=4) as downloader:
    future = [downloader.submit(downloadMap, mapType, scene['sceneId'], extent, token) 
                for scene in scenes]
    for future in concurrent.futures.as_completed(future):
      try:
        imageryMap = future.result()
        maps.append(imageryMap)
      except SpaceKnowError as e:
        spaceKnowLogger.error("Error %d during imagery map download: %s" % 
                                    (e.status_code, e.error))
      except Exception as e:
        spaceKnowLogger.error("Unknown error during imagery map download: %s"% e)
  
  return maps

def downloadMap(mapType, scene, extent, token):
  url = buildURL(utils.SK_KRAKEN_API, 'release', mapType, 'geojson')
  data = json.dumps({'sceneId': scene,
                     'extent': extent})
  try:
    pipeline = Pipeline(url, token, data)
    pipeline.start()
    spaceKnowLogger.info('Making Request for scene %s' % scene)
    jsonMap = pipeline.join()
    if not jsonMap or 'mapId' not in jsonMap or 'maxZoom' not in jsonMap or \
      'tiles' not in jsonMap:
      raise SpaceKnowError('Receive invalid map for scene %s' % scene, 500)
    return jsonMap
  except SpaceKnowError as e:
    spaceKnowLogger.error('Error %d: %s' % (e.status_code, e.error))

class Tile():
  def __init__(self, tile:list):
    self.z = str(tile[0])
    self.x = str(tile[1])
    self.y = str(tile[2])

  def __str__(self):
    return '_'.join([self.z, self.x, self.y])
  
  def aslist(self):
    return [self.z, self.x, self.y]

class KrakenObject():
  def __init__(self, mapType, geometry_id='-', outputDir='output'):
    validateMap(mapType)
    self.mapType = mapType
    self.resources = KRAKEN_MAPS[mapType]
    self._url = buildURL(utils.SK_KRAKEN_API, 'grid')
    self._geometryId = '-'
    self.outputDir = outputDir

  def download_resource(self, mapId, tile, resource):
    validateResource(self.mapType, resource)
    try:
      tileUrl = buildURL(self._url, mapId, self._geometryId, 
                              tile.z, tile.x, tile.y, resource)
      spaceKnowLogger.info('GET %s' % tileUrl)
      if resource.endswith('.png'):
        image = Image.open(requests.get(tileUrl, stream=True).raw)
        return image
      elif resource.endswith('.json'):
        return utils.process(tileUrl, '', isGET=True)
      elif resource.endswith('.geojson'):
        jsonFile = utils.process(tileUrl, isGET=True)
        if 'features' not in jsonFile:
          spaceKnowLogger.error("Invalid resource from %s"% tileUrl)
          return None
        else:
          return jsonFile['features']
      elif resource in self.resources :
        raise SpaceKnowError("Resource not available at the moment", 404)
      else:
        raise SpaceKnowError("Unknown resource for the object %s"% self.mapType, 404)
    except Exception as e:
      spaceKnowLogger.error("Error downloading resource %s: %s" %(resource, e))

  
  def download_tiles(self, mapId, tiles, resource):
    tilesResource = {}
    for tile in tiles:
      try:
        if len(tile) != 3:
          spaceKnowLogger.error('Invalid tile for map %s' % mapId, 400)
        else:
          t = Tile(tile)
          tileRes = self.download_resource(mapId, t, resource)
          tilesResource[str(t)] = tileRes
      except SpaceKnowError:
        spaceKnowLogger.error("Error downloading resource %s for mapId %s" %
                              (resource, mapId))
    return tilesResource
  
  def build_png(self, mapId, tiles, resource, outputFile):
    tiledMapPath = path.join(self.outputDir)
    images = []
    if not path.exists(tiledMapPath):
      os.mkdir(tiledMapPath)
    
    images = []
    for tile in tiles:
      if type(tile) == list:
        tile = Tile(tile)
      image = self.download_resource(mapId, tile, resource)
      images.append(image)
    
    utils.stitchImages(images, filename=path.join(tiledMapPath, 
                                                    outputFile))


class CarsObject(KrakenObject):
  def __init__(self, mapType='cars'):
    super().__init__(mapType)
  
  def detectCars(self, mapId, tiles):
    """ Check if there is a group of Cars inside a Map
      
      Arguments:
      - mapId: identifier of imagery got from KRAKEN API Imagery
      - tiles: list of tile inside the imagery

      Returns:
      - tiles_with_cars: list of tiles where there is AT LEAST 1 Car
      - countedCars: number of cars inside mapId
    """
    countedCars = 0
    tiles_with_cars = []
    detectionsAnalysis = self.download_tiles(mapId, tiles, resource='detections.geojson')
    
    for tile in tiles:
      t = Tile(tile)
      count = 0
      if str(t) not in detectionsAnalysis:
        continue
      else:
        jsonAnalysis = detectionsAnalysis[str(t)]
        for jsonObj in jsonAnalysis:
          if 'properties' not in jsonObj:
            raise SpaceKnowError('Invalid Resource for tile %s'% str(t), 500)
          else:
            count+= jsonObj['properties']['count']
      if count > 0:
        tiles_with_cars.append(t)
        countedCars += count

    return countedCars, tiles_with_cars


class KrakenManager():
  def __init__(self, logger=spaceKnowLogger):
    self.logger = logger

  def process(self, maps, operation):
    validateOperations(operation)
    result = {}
    if operation == 'CAR_DETECTION':
      for imagery in maps:
        self.logger.info("Detecting cars for map %s"% imagery['mapId'] )
        carsDetector = CarsObject()
        counted_cars, tiles = carsDetector.detectCars(imagery['mapId'], 
                                                      imagery['tiles'])
        if counted_cars > 0:
          result[imagery['mapId']] = (counted_cars, tiles)
    elif operation == 'BUILD_CARS_PNG':
      for imagery in maps:
        self.logger.info("Creating PNG file for %s"% imagery['mapId'])
        imageGenerator = KrakenObject(mapType='cars')
        imageGenerator.build_png(imagery['mapId'], imagery['tiles'], 'cars.png',
                              imagery['mapId'][-10:]+'_detection.png')
    elif operation == 'BUILD_PNG':
      for imagery in maps:
        self.logger.info("Creating PNG file for %s"% imagery['mapId'])
        imageGenerator = KrakenObject(mapType='imagery')
        imageGenerator.build_png(imagery['mapId'], imagery['tiles'], 'truecolor.png',
                              imagery['mapId'][-10:]+'_imagery.png')
    return result
  
  def build_image(self, mapId, tiles, operation):
    validateOperations(operation)
    if operation == 'BUILD_CARS_PNG':
      self.logger.info("Creating PNG file for %s"% mapId[-10:])
      imageGenerator = KrakenObject(mapType='cars')
      imageGenerator.build_png(mapId, tiles, 'cars.png',
                               mapId[-10:]+'_detection.png')
    elif operation == 'BUILD_PNG':
      self.logger.info("Creating PNG file for %s"% mapId[-10:])
      imageGenerator = KrakenObject(mapType='imagery')
      imageGenerator.build_png(mapId, tiles, 'truecolor.png',
                            mapId[-10:]+'_imagery.png')