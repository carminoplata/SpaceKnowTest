import json
import logging
import logging.config
import urllib.request

from flask import Flask, request, jsonify


logging.config.fileConfig('logging.conf')
logger = logging.getLogger('SpaceKnow')

# SpaceKnow AUTH0 Credentials needs to generate JWT (JSON Web Token)
SPACEKNOW_AUTH0_DOMAIN = 'spaceknow.auth0.com'
SPACEKNOW_CLIENT_ID = 'hmWJcfhRouDOaJK2L8asREMlMrv3jFE1'

app = Flask(__name__)
app.config["DEBUG"] = True

class SpaceKnowManager:

  def __init__(self):
    logger.debug('Initialize SpaceKnowManager')

@app.route('/')
def hello_world():
  return 'Hello World!'

app.run()