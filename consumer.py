import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from time import sleep
import pandas as pd
from pycaret.anomaly import *
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_squared_error

# #model = linear_model.LinearRegression()
# model = tree.HoeffdingTreeRegressor(
#         grace_period=100,
#         model_selector_decay=0.9
#     )

class AirCompressor(object):
    """
    User record

    Args:
        {
          "symbol": "BTCUSDT",
          "openPrice": "61729.27000000",
          "highPrice": "61800.00000000",
          "lowPrice": "61319.47000000",
          "lastPrice": "61699.01000000",
          "volume": "814.22297000",
          "quoteVolume": "50138059.82771860",
          "openTime": 1715732880000,
          "closeTime": 1715736489761,
          "firstId": 3599114332,
          "lastId": 3599147596,
          "count": 33265
        }
    """

    def __init__(self, timestamp, tp2, tp3, h1, dv_pressure, reservoirs, oilTemperature, motorCurrent, comp,dvEletric,towers,mpg,lps,pressureSwitch,oilLevel,caudalImpulses):
        self.timestamp = timestamp
        self.tp2 = tp2
        self.tp3 = tp3
        self.h1 = h1
        self.dv_pressure = dv_pressure
        self.reservoirs = reservoirs
        self.oilTemperature = oilTemperature
        self.motorCurrent = motorCurrent
        self.comp = comp
        self.dvEletric = dvEletric
        self.towers = towers
        self.mpg = mpg
        self.lps = lps
        self.pressureSwitch = pressureSwitch
        self.oilLevel = oilLevel
        self.caudalImpulses = caudalImpulses

def sensor_to_dict(sensor, ctx):
    if sensor is None:
        return None
   
    # User._address must not be serialized; omit from dict
    return AirCompressor(timestamp=sensor['timestamp'], tp2=sensor['tp2'], tp3=sensor['tp3'], h1=sensor['h1'],
                dv_pressure=sensor['dv_pressure'], reservoirs=sensor['reservoirs'], oilTemperature=sensor['oilTemperature'], motorCurrent=sensor['motorCurrent'],
                comp=sensor['comp'],dvEletric=sensor['dvEletric'],towers=sensor['towers'],mpg=sensor['mpg'],lps=sensor['lps'],
                pressureSwitch=sensor['pressureSwitch'],oilLevel=sensor['oilLevel'],caudalImpulses=sensor['caudalImpulses'])




def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        schema = "sensor_specific.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         sensor_to_dict)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "latest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            airCompressor = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            '''if airCompressor is not None:
                print("User record {}: symbol: {}"
                      "\topenPrice: {}"
                      "\thighPrice: {}"
                      "\tlowPrice: {}"
                      "\tvolume: {}"
                      "\topenTime: {}"
                      "\tcloseTime: {}"
                      "\tcount: {}"
                      "\tlastPrice: {}\n"
                      .format(msg.key(), 
                              user.symbol,
                              user.openPrice,
                              user.highPrice, 
                              user.lowPrice, 
                              user.volume, 
                              user.openTime, 
                              user.closeTime, 
                              user.count,
                              user.lastPrice)
                    )'''
                
            # Create a dataframe for the consuming data to feed into the ML model.
            # For example
        #      self.timestamp = timestamp
        # self.tp2 = tp2
        # self.tp3 = tp3
        # self.h1 = h1
        # self.dv_pressure = dv_pressure
        # self.reservoirs = reservoirs
        # self.oilTemperature = oilTemperature
        # self.motorCurrent = motorCurrent
        # self.comp = comp
        # self.dvEletric = dvEletric
        # self.towers = towers
        # self.mpg = mpg
        # self.lps = lps
        # self.pressureSwitch = pressureSwitch
        # self.oilLevel = oilLevel
        # self.caudalImpulses = caudalImpulses
            if airCompressor is not None:
                data = {'timestamp':airCompressor.timestamp, 'TP2':airCompressor.tp2,
                    'TP3':airCompressor.tp3,'H1':airCompressor.h1,'DV_pressure':airCompressor.dv_pressure,
                    'Reservoirs':airCompressor.reservoirs,'Oil_temperature':airCompressor.oilTemperature,
                    'Motor_current':airCompressor.motorCurrent,'COMP':airCompressor.comp,'DV_eletric':airCompressor.dvEletric,
                    'Towers':airCompressor.towers,'MPG':airCompressor.mpg,'LPS':airCompressor.lps,'Pressure_switch':airCompressor.pressureSwitch,
                    'Oil_level':airCompressor.oilLevel,'Caudal_impulses':airCompressor.caudalImpulses
                    }
            
                df = pd.DataFrame(data,index=[airCompressor.timestamp])

                #print(df)

            #Predictions
                loaded_model = load_model('knn_pipeline')
                predictions = predict_model(loaded_model, data=df)
            
                
                print("Predicted", predictions.head())
            #print(mean_squared_error([user.lastPrice] , [predictions.iloc[0]['prediction_label']] ) )
            

           

           



        except KeyboardInterrupt:
            break

        sleep(5)

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())


# Example
# python consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "airCompressor" -g "airCompressor"