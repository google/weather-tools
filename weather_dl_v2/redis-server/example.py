import asyncio
import json
import redis.asyncio as redis

STOPWORD = "STOP"

async def reader(channel: redis.client.PubSub):
    while True:
        message = await channel.get_message(ignore_subscribe_messages=True)
        if message is not None:
            print(f"(Reader) Message Received: {message}")
            if message["data"].decode() == STOPWORD:
                print("(Reader) STOP")
                break

async def main():

    r = redis.from_url("redis://localhost")

    async with r.pubsub() as pubsub:
        await pubsub.subscribe("channel:1")

        future = asyncio.create_task(reader(pubsub))
        res = {"selection": {"class": "od", "type": "pf", "stream": "enfo", "expver": "0001", "levtype": "pl", "levelist": "100", "param": "129.128", "date": ["2019-07-22"], "time": "0000", "step": ["0", "1", "2"], "number": ["1", "2"], "grid": "F640"}, 
            "user_id": "mahrsee", 
            "url": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "target_path": "XXXXX"}
        data_str = json.dumps(res)
        
        await r.publish("channel:1", data_str)
        # await r.publish("channel:1", STOPWORD)
        
        await future
        
if __name__ == "__main__":
  asyncio.run(main())
