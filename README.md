# MongoDB to Spark connector example

This project demonstrate how to use the MongoDB to Spark connector.

The queries are adapted from the [aggregation pipeline example](https://docs.mongodb.com/v3.2/tutorial/aggregation-zip-code-data-set/) from the MongoDB documentation.

# How to run:

## Prerequisite:

- Install docker and docker-compose
- Install maven

## Run MongoDB and import data

From the project root:
    
    docker-compose -f docker/docker-compose.yml up -d
    docker exec -it mongo_container sh /scripts/import-data.sh


## Checking:

Verify the data have been loaded in MongoDB by connecting to the container and run a count:
	
	docker exec mongo_container mongo --eval "db.zips.count()"


Should return:

    MongoDB shell version: 3.2.11
    connecting to: test
    29353


## Import the project

- Import the maven project in your favorite IDE
- Run the MongoSparkMain class