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

class MapsQueue(Queue):
  def getTiles(self):
    tileURLs = []
    for tiledMap in self.queue:
      mapId = tiledMap['mapId']
      tiles = tiledMap['tiles']
      for tile in tiles:
        z = str(tile[0])
        x = str(tile[1])
        y = str(tile[2])
        tileURLs.append(buildURL(mapId, '-',z,x,y))
    return tileURLs

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
      image = self.download_resource(mapId, Tile(tile), resource)
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
                              imagery['mapId'][:10]+'_detection.png')
    elif operation == 'BUILD_PNG':
      for imagery in maps:
        self.logger.info("Creating PNG file for %s"% imagery['mapId'])
        imageGenerator = KrakenObject(mapType='imagery')
        imageGenerator.build_png(imagery['mapId'], imagery['tiles'], 'truecolor.png',
                              imagery['mapId'][:10]+'_imagery.png')
    return result

class KrakenProducer():
  def __init__(self, mapType, scenes, extent, tiledMaps, token):
    #super().__init__()
    self.url = utils.SK_KRAKEN_API + '/release/' + mapType + '/geojson'
    self.type = mapType
    self.scenes = scenes
    self.extent = extent
    self.queue = tiledMaps
    self.token = token
    self.error = None

  def __makeRequest(self, scene):
    data = json.dumps({'sceneId': scene,
                       'extent': self.extent})
    pipeline = Pipeline(self.url, self.token, data)
    pipeline.start()
    print("Making Request for scene %s" % scene)
    mapTile =  pipeline.join()
    if not mapTile or 'mapId' not in mapTile or 'maxZoom' not in mapTile or \
      'tiles' not in mapTile:
      raise SpaceKnowError('Receive invalid map for scene %s' % scene, 500)
    return mapTile
  
  def run(self):
    try:
      for scene in self.scenes:
        if 'sceneId' not in scene:
          print('Error Invalid scene')
        else:  
          if not self.queue.full():
            mapTile = self.__makeRequest(scene['sceneId'])
            self.queue.put(mapTile, block=True)
    except SpaceKnowError as e:
      self.error = e
  
  def join(self):
    #super().join()
    if self.error:
      return self.error

class KrakenConsumer():
  """ Should call GET /kraken/grid/<map_id>/<geometry_id>/<z>/<x>/<y>/<file_name>
  """
  def __init__(self, token, tiledMaps, resource, geometry_id='-', outputDir=''):
    #super().__init__()
    self.url = buildURL(utils.SK_KRAKEN_API, 'grid')
    self.queue = tiledMaps
    self.token = token
    self.geometry_id = geometry_id
    self.filename = resource
    self.outputDir = outputDir
    self.error = None
    
  
  def __processMap(self, tiledMap, index, outputFile):
    if self.filename.endswith('.png'):
      self.__processOutputAsPNG(tiledMap, index, outputFile)
    elif self.filename.endswith('.geotiff'):
      pass
    elif self.filename.endswith('.ski'):
      pass
    else:
      raise SpaceKnowError("Unknown resource. Kraken provides only png, geotiff and ski "
                           "files", 400)
      
  
  def __processOutputAsPNG(self, tiledMap, index, outputFile):
    tiledMapPath = path.join(self.outputDir, str(index))
    images = []
    if not path.exists(tiledMapPath):
      os.mkdir(tiledMapPath)
    try:
      for tile in tiledMap['tiles']:
        z = str(tile[0])
        x = str(tile[1])
        y = str(tile[2])
        tileUrl = buildURL(self.url, tiledMap['mapId'], self.geometry_id, 
                            z, x, y, self.filename)
        print("GET %s"% tileUrl)
        img = Image.open(requests.get(tileUrl, stream=True).raw)
        img.show()
        images.append(img)

      outputFile = '_'.join([str(index), outputFile])
    
      utils.stitchImages(images, filename=path.join(tiledMapPath, 
                                                    outputFile))
    except SpaceKnowError as e:
      print("Error for getting the tile %s "% tileUrl)
      print("Error %d: %s"%(e.status_code, e.error))
    except OSError as e:
      print("Impossible to open %s"% tileUrl)

  def run(self, outputFile):
    #if not self.queue.empty():
    for i,tiledMap in enumerate(self.queue.queue):
      if 'mapId' not in tiledMap or 'tiles' not in tiledMap:
        print("Error invalid map")
      elif len(tiledMap['tiles'])==0:
        print("There is not any tile for map %s" % tiledMap['mapId'])
      else:
        self.__processMap(tiledMap, i, outputFile)
  """
  def getPng(self, name, outputFile):
    resourceFile = ".".join(name, 'png')
    self.run(resourceFile, outputFile)
  """
  #def join(self):
    #super().join()
    #return self.__result