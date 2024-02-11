import redis
from aiohttp import web


async def get_popular_destination(request):
    redis_client = redis.Redis(host='localhost', port=6379)

    pulocation = int(request.query.get('pulocation'))
    keys = await redis_client.get(f'{pulocation}:*')
    values = await redis_client.mget(*keys)
    max_value = max(map(int, values))
    max_key = keys[values.index(str(max_value))]
    redis_client.close()
    doulocation = max_key.split(':')[1]
    return web.Response(text=doulocation)


async def main_logic(pulocation):
    app = web.Application()
    app.router.add_get(
        f'/popular_destination/{pulocation}', get_popular_destination
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
