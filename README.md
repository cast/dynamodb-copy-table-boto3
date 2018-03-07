# Make copy of DynamoDB table

Author: Jon Austen

Warning:  don't use Python 2.x

Requires :

    $ curl -O https://bootstrap.pypa.io/get-pip.py
    $ python3 get-pip.py --user
    $ pip3 install awscli --upgrade --user

### HOWTO

This is a python script to make copy of AWS Dynamodb tables using boto3 library.

 Copy a online dynamo db:

    python3 dynamo_copy_table.py atable copy-atable false


### NOTES

https://stackoverflow.com/questions/31378347/how-to-get-the-row-count-of-a-table-instantly-in-dynamodb

