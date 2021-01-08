import json
import utils

from pipeline import Pipeline
from queue import Queue
from threading import Thread
from utils import SpaceKnowError, process

class MapsQueue(Queue):
  pass

class KrakenProducer(Thread):
  def __init__(self, mapType, scenes, extent, tiledMaps, token):
    super().__init__()
    self.url = utils.SK_KRAKEN_API + '/release/mapType/geojson/'
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
        if not self.queue.full():
          mapTile = self.__makeRequest(scene)
          self.queue.put(mapTile, block=True)
    except SpaceKnowError as e:
      self.error = e
  
  def join(self):
    super().join()
    if self.error:
      return self.error

class KrakenConsumer(Thread):
  """ Should call GET /kraken/grid/<map_id>/<geometry_id>/<z>/<x>/<y>/<file_name>
  """
  def __init__(self, tiledMaps, token, geometry_id='-', filename='cars.png'):
    super().__init__()
    self.url = utils.SK_KRAKEN_API + '/grid/'
    self.queue = tiledMaps
    self.token = token
    self.__result = []
    self.geometry_id = geometry_id
    self.filename = filename
    self.error = None
  
  def __makeRequest(self, mapId, tiles):
    for tile in tiles:
      tileUrl = self.url + mapId + '/' + self.geometry_id + tile[0] + '/' + \
        tile[1] + '/' + tile[2] + '/' + self.filename
      try:
        response = process(tileUrl, token=self.token, isGET=True)
        return response
      except SpaceKnowError as e:
        print("Error for getting the tile %s "% tileUrl)
        print("Error %d: %s"%(e.status_code, e.error))

  def run(self):
    if not self.queue.empty():
      tiledMap = self.queue.get()
      if 'mapId' not in tiledMap or 'tiles' not in tiledMap:
        print("Error invalid map")
      elif len(tiledMap)==0:
        print("There is not any tile for map %s" % tiledMap['mapId'])
      else:
        tile = self.__makeRequest(tiledMap['mapId'], tiledMap['tiles'])
        self.__result.append(tile)
  
  def join(self):
    super().join()
    return self.__result
    

  

  

