import json
import logging
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return func.HttpResponse(json.dumps({'message':'Hello World'}),
                             status_code=200,
                             mimetype='application/json')
