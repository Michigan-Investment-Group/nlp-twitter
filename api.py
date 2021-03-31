from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from crawler import TwitterCrawler
import logging
import time 

app = Flask(__name__)
CORS(app)

#configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

twitter = TwitterCrawler()

@app.route('/twitter', methods=['GET'])
def process_twitter():
    twitter.crawl()
    logger.info("Process twitter called at time %s", time.now())
    response = make_response(jsonify({
        "task": "Twitter Crawling",
        "status": "Completed"
    }))
    logger.info("Response: %s", response)
    return response
    


@app.route('/crawl_ticker', methods=['GET'])
def handle_twitter_ticker(): 
    ticker = request.args.get('ticker')
    data = twitter.crawl_ticker(ticker)
    logger.info("handle_twitter_ticker() with argument {ticker} called at time %s", time.now())
    response = make_response(jsonify(data))
    logger.info("Response: %s", response)
    return response
# ==================== DO NOT ALTER THIS ====================

if __name__ == '__main__':
    PORT = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=PORT)
