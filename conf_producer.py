from confluent_kafka import Producer
import json

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

userdata = {'userid':22,'command':'create_address', 'coin': "BSC","userobj": "60489l7fbfd785108885026d", "assetobj": "5df777762be0fe0011cb0135" }
p.poll(5)
data = json.dumps(userdata)
p.produce('wallet-BSCaddr-command', data.encode('utf-8'), callback=delivery_report)

p.flush()