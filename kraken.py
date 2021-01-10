import json
import os
import requests
import utils

from io import BytesIO
from os import path
from PIL import Image
from pipeline import Pipeline
from queue import Queue
from threading import Thread
from utils import SpaceKnowError, process, buildURL

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