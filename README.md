This script will automate the rollover and aliasing of indices in ElasticSearch to attempt to standardize all indices at 100gb for making 50gb shards when doing 2 primary shards. It will also alias for searching purposes if using date stamps

This was originally an AWS Lambda function. 
