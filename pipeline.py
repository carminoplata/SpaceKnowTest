import json
import time
import utils

from threading import Thread
from utils import process, SpaceKnowError

class Pipeline(Thread):
  def __init__(self, url, token, request):
    super().__init__()
    self.url = url
    self.token = token
    self.request = request
    self.__return = None
    self.error = None

  def __initiate(self):
    utils.logger.info("Initiate pipeline at %s" % self.url)
    try:
      response = process(self.url+'/initiate', data=self.request, token=self.token)
      if 'pipelineId' not in response or 'nextTry' not in response or 'status' not in response:
        raise SpaceKnowError('Invalid response', 500)
      if response['status'] == 'FAILED':
        raise SpaceKnowError('Error during pipeline processing', 500)
      elif response['status'] == 'PROCESSING' or response['status'] == 'NEW':
        return response['nextTry'], response['pipelineId']
      #elif response['status'] == 'RESOLVED':
        # call retrieve
      else:
        raise SpaceKnowError('Invalid status {}'.format(response['status']), 500)
    except SpaceKnowError as e:
      utils.logger.error("Error %d during initiate phase: %s" %
                    (e.status_code, e.error))
      self.error = e
      raise e
  
  def __isReady(self):
    try:
      url = utils.SK_TASK_API +'/get-status'
      pipelineId = json.dumps({"pipelineId": self.id})
      response = process(url, data=pipelineId, token=self.token)
      if 'status' not in response or \
        (response['status']!='RESOLVED' and 'nextTry' not in response) :
        raise SpaceKnowError(('Invalid response during checking the pipeline\'s status: %s', 
                        pipelineId), 500)
      if response['status'] == 'RESOLVED':
        return True
      elif response['status'] == 'FAILED':
        raise SpaceKnowError('Error during pipeline processing', 500)
      else:
        self.nextTry= response['nextTry']
        return False
    except SpaceKnowError as e:
      utils.logger.error("Error %d during status checking at pipeline %s: %s" % 
                    (e.status_code, self.id, e.error))
      self.error = e
      raise e

  def __retrieve(self):
    pipelineId = json.dumps({"pipelineId": self.id})
    response = process(self.url+'/retrieve', data=pipelineId, token=self.token)
    return response
    
  """  
  #status = Event()
  time.sleep(tryIn)
  try:
    while not isReady(pipelineId, tryIn, token):
      print("Imagery is not available yet. Retry in %d"% tryIn)
      time.sleep(tryIn)
  except SpaceKnowError as e:
     logger.error("Error %d during status checking at pipeline %s: %s" % 
                    (e.status_code, pipelineId, e.error))
"""
  def run(self):
    try:
      self.nextTry, self.id = self.__initiate()
      time.sleep(self.nextTry)
      while not self.__isReady():
        utils.logger.info("Imagery is not available yet. Retry in %d"% self.nextTry)
        time.sleep(self.nextTry)
      self.__return = self.__retrieve()
    except SpaceKnowError as e:
        utils.logger.error("Error %d at pipeline %s: %s" % 
                      (e.status_code, self.url, e.error)) 

  def join(self):
    super().join()
    if self.error:
      raise self.error

    return self.__return
