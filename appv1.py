import websocket
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml.regression import LinearRegression

api_key = "ceqjsr2ad3i9f7a52g10ceqjsr2ad3i9f7a52g1g"

# Create a SparkSession
spark = SparkSession.builder.appName("My App").getOrCreate()

# Set up the Spark streaming context
sc = SparkContext(appName="FinnhubStream")
ssc = StreamingContext(sc, 1) 

# Create a DStream that receives data over a network connection
lines = ssc.socketTextStream("hostname", 9999)

# Set up the WebSocket connection
def on_message(ws, message):
    message = json.loads(message)
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("WebSocket connection closed")

def on_open(ws):
    # Subscribe to a stream
    subscribe_request = {
        "type": "subscribe",
        "symbol": "AAPL",
        "resolution": "D"
    }
    ws.send(json.dumps(subscribe_request))

ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=" + api_key,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

# Start the WebSocket connection in a separate thread
ws.run_forever()

# Create a DStream from the WebSocket connection
dstream = ssc.socketTextStream(ws)

# Define a function to process the data in the DStream
def process_stream(time, rdd):
    # Convert the RDD to a DataFrame
    df = rdd.toDF(["close"])

    # Add a time column to the DataFrame
    df = df.withColumn("time", time)

    # Split the data into a training set and a test set
    train_df, test_df = df.randomSplit([0.9, 0.1])

    # Build a linear regression model
    lr = LinearRegression()
    model = lr.fit(train_df)

    # Make predictions on the test set
    predictions = model.transform(test_df)

    # Print the predictions
    predictions.select("prediction").show()

# Process the stream
dstream.foreachRDD(process_stream)

# Start the streaming context and process the stream
ssc.start()
ssc.awaitTermination()

# Stop the streaming context and close the WebSocket connection
ssc.stop()
ws.close()
