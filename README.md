# SpaceKnow 

The goal of this project is to provide a solution to count the numbers of car inside BNE Staff Carpark near the Brisbane Airpor (Australia) using SpaceKnow API. 
More details can be found inside `SpaceKnow Backend Candidate Assignment.pdf`

## Requirements

Check your python version. It requires at least Python 3.
Before you run the script, you need to install all libraries provided into requirements.txt, using
`pip install -r requirements.txt`

Inside requirements.txt, you will find:

* geojson: python library for encoding and decoding JSON Data
* python-dotenv: module python for using `.env` configuration file
* requests: python library to make and manage HTTP Request
* Pillow: python library for image processing
 
## How to Run:
Go to project folder and run:
`python3 spaceknow.py`

If you want, you could try to provide a different account and area to detect running:

`python3 spaceknow.py <username> <password> <geojsonfile>`

**WARNING** Some bugs could rise up. Please provides me spaceknow.log and console output

The script counts the number of cars in the area detected by geojson file (default i s Brisbane Staff Car Park airport) and will create inside `output` folder a set of detection and satellite images used for the analysis.



## Design Script
Inside the project, there are the following files:

* `.env`: configuration file for creating constants used for authenticate and communicate with SpaceKnow API
* `logging.conf`: configuration file for logging management
* `freeArea.geojson`: area without any imageries
* `over_brisbane_airport.geojson`: area over Staff Park Lot near Brisbane Airport
* `utils.py`: module where are defined global function used in several modules
* `pipeline.py`: python module for creating Pipeline class which manages the whole lifecycle of SpaceKnow's Pipeline
* `kraken.py`: python module for the management of Kraken API. It defines:
  + *Tile*: It define a single Tile as its components z, x, y
  + *KrakenObject*: It defines a global Object which manages the downloading and the process of a particular Kraken resource provided by https://api.spaceknow.com/kraken/grid
  + *CarsObject*: It is a child class of KrakenObject to manages the car detection
  + *KrakenManager*: It is a manager to process a KrakenOperation in according to the object desired.
* `spaceknow.py`: main of the application. It runs cars detection just calling 1 function.

## Future Improvements

If you need to extend or you need to define new features using SpaceKnow.py, you should follow these steps in kraken.py:

1) Define the new operation into KRAKEN_OPERATIONS list 
2) Define a new object as child of KrakenObject if there is not any KrakenObject already available for your mapType, otherwise extend features of that object
3) Updates KrakenManager process to manage your particular operation

## Limitations

Actually the script is able to process Imagery only from `idaho-pansharpened` database and `gbdx` provider.
Besides it is able to:
* detect only the cars inside an area
* process PNG and Json/GeoJson files as resources for each tile inside a grid

Actually there is a security problem, all user data are provided without encryption alogrithm.
