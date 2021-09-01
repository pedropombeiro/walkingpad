from aiohttp import web
from bleak.exc import BleakError
from ph4_walkingpad import pad
from ph4_walkingpad.pad import WalkingPad, Controller
from ph4_walkingpad.utils import setup_logging
import asyncio
import yaml
import psycopg2
from datetime import date

routes = web.RouteTableDef()
connectTask = None

# minimal_cmd_space does not exist in the version we use from pip, thus we define it here.
# This should be removed once we can take it from the controller
minimal_cmd_space = 0.69

log = setup_logging()
pad.logger = log
ctler = Controller()

last_status = {
    "steps": None,
    "distance": None,
    "time": None
}

def on_new_status(sender, record):

    distance_in_km = record.dist / 100
    print("Received Record:")
    print('Distance: {0}km'.format(distance_in_km))
    print('Time: {0} seconds'.format(record.time))
    print('Steps: {0}'.format(record.steps))

    last_status['steps'] = record.steps
    last_status['distance'] = distance_in_km
    last_status['time'] = record.time


def store_in_db(steps, distance_in_km, duration_in_seconds):
    db_config = load_config()['database']
    if not db_config['host']:
        return

    try:
        conn = psycopg2.connect(host=db_config['host'], port=db_config['port'],
                                dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'])
        cur = conn.cursor()

        date_today = date.today().strftime("%Y-%m-%d")
        duration = int(duration_in_seconds / 60)

        cur.execute("INSERT INTO exercise VALUES ('{0}', {1}, {2}, {3})".format(
            date_today, steps, duration, distance_in_km))
        conn.commit()

    finally:
        cur.close()
        conn.close()


def load_config():
    with open("config.yaml", 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def save_config(config):
    with open('config.yaml', 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


async def done():
    # No- op
    return

async def ensureConnected(attempt=0):
    global connectTask

    print("- Attempt #{0}".format(attempt+1))
    if connectTask is None:
        connectTask = connect()

    try:
        await connectTask
        connectTask = done()
    except BleakError as e:
        if attempt < 1:
            await ctler.disconnect()

            connectTask = connect()
            await ensureConnected(attempt+1)
        else:
            print("Giving up retrying. Raising '{0}'".format(e))
            connectTask = None
            raise e
    except Exception as e:
        print("- Unhandled exception: {0}".format(e))
        connectTask = None
        raise e

async def connect():
    address = load_config()['address']
    print("Connecting to {0}".format(address))
    await ctler.run(address)
    await asyncio.sleep(minimal_cmd_space)


async def disconnect():
    global connectTask
    connectTask = None

    await ctler.disconnect()
    await asyncio.sleep(minimal_cmd_space)


@routes.get("/config/address")
def get_config_address(request):
    config = load_config()
    return str(config['address']), 200


@routes.post("/config/address")
def set_config_address(request):
    address = request.rel_url.query.get('address', '')
    config = load_config()
    config['address'] = address
    save_config(config)

    return get_config_address()


async def _get_pad_mode():
    await ctler.ask_stats()
    await asyncio.sleep(minimal_cmd_space)
    stats = ctler.last_status
    mode = stats.manual_mode

    if (mode == WalkingPad.MODE_STANDBY):
        return web.json_response({ "mode": "standby" })
    elif (mode == WalkingPad.MODE_MANUAL):
        return web.json_response({ "mode": "manual" })
    elif (mode == WalkingPad.MODE_AUTOMAT):
        return web.json_response({ "mode": "auto" })

    return web.HTTPBadRequest(text="Mode {0} not supported".format(mode))

@routes.get("/mode")
async def get_pad_mode(request):
    try:
        await ensureConnected()

        return await _get_pad_mode()
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

@routes.post("/mode")
async def change_pad_mode(request):
    new_mode = request.rel_url.query.get('new_mode', 'manual')
    print("Got mode {0}".format(new_mode))

    if (new_mode.lower() == "standby"):
        pad_mode = WalkingPad.MODE_STANDBY
    elif (new_mode.lower() == "manual"):
        pad_mode = WalkingPad.MODE_MANUAL
    elif (new_mode.lower() == "auto"):
        pad_mode = WalkingPad.MODE_AUTOMAT
    else:
        return web.HTTPBadRequest(text="Mode {0} not supported".format(new_mode))

    try:
        await ensureConnected()

        await ctler.switch_mode(pad_mode)
        await asyncio.sleep(minimal_cmd_space)

        return await _get_pad_mode()
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

@routes.post("/speed")
async def change_speed(request):
    value = request.rel_url.query.get('value', '')
    print("Setting speed to {0} km/h".format(value))

    try:
        await ctler.change_speed(int(float(value) * 10))

        await asyncio.sleep(ctler.minimal_cmd_space)
        await ctler.ask_stats()
        await asyncio.sleep(minimal_cmd_space)
        stats = ctler.last_status

        return web.json_response({ "speed": stats.app_speed / 30 })
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

@routes.post("/pref/{name}")
async def change_pref(request):
    name = request.match_info.get('name', '')
    value = request.rel_url.query.get('value', '')
    print("Setting preference {0} to {1}".format(name, value))

    try:
        await ensureConnected()

        if (name == "initial-speed"):
            ctler.set_pref_start_speed(value * 10)
        elif (name == "max-speed"):
            ctler.set_pref_max_speed(value * 10)
        elif (name == "sensitivity"):
            ctler.set_pref_sensitivity(value)
        else:
            return web.HTTPBadRequest(text="Preference {0} not supported".format(name))
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

    await asyncio.sleep(ctler.minimal_cmd_space)

@routes.get("/status")
async def get_status(request):
    try:
        await ensureConnected()

        await ctler.ask_stats()
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

    stats = ctler.last_status
    mode = stats.manual_mode
    belt_state = stats.belt_state

    if (mode == WalkingPad.MODE_STANDBY):
        mode = "standby"
    elif (mode == WalkingPad.MODE_MANUAL):
        mode = "manual"
    elif (mode == WalkingPad.MODE_AUTOMAT):
        mode = "auto"

    if (belt_state == 5):
        belt_state = "standby"
    elif (belt_state == 0):
        belt_state = "idle"
    elif (belt_state == 1):
        belt_state = "running"
    elif (belt_state >=7):
        belt_state = "starting"

    dist = stats.dist / 100
    time = stats.time
    steps = stats.steps

    return web.json_response({ "dist": dist, "time": time, "steps": steps, "speed": stats.app_speed / 30, "belt_state": belt_state, "mode": mode })


@routes.get("/history")
async def get_history(request):
    try:
        await ensureConnected()

        await ctler.ask_hist(0)
        await asyncio.sleep(minimal_cmd_space)

        return web.json_response(last_status)
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

@routes.post("/save")
def save(request):
    store_in_db(last_status['steps'], last_status['distance'], last_status['time'])

@routes.post("/startwalk")
async def start_walk(request):
    try:
        await ensureConnected()

        await ctler.switch_mode(WalkingPad.MODE_STANDBY) # Ensure we start from a known state, since start_belt is actually toggle_belt
        await asyncio.sleep(minimal_cmd_space)
        await ctler.switch_mode(WalkingPad.MODE_MANUAL)
        await asyncio.sleep(minimal_cmd_space)
        await ctler.start_belt()
        await asyncio.sleep(minimal_cmd_space)
        await ctler.ask_hist(0)
        await asyncio.sleep(minimal_cmd_space)

        return web.json_response(last_status)
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))

@routes.post("/finishwalk")
async def finish_walk(request):
    try:
        await ensureConnected()

        await ctler.stop_belt()
        await asyncio.sleep(ctler.minimal_cmd_space)
        await ctler.switch_mode(WalkingPad.MODE_STANDBY)
        await asyncio.sleep(minimal_cmd_space)
        await ctler.ask_hist(0)
        await asyncio.sleep(minimal_cmd_space)
        store_in_db(last_status['steps'], last_status['distance'], last_status['time'])

        return web.json_response(last_status)
    except BleakError as e:
        await ctler.disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))


async def app_factory():
    global connectTask
    global ctler

    ctler.handler_last_status = on_new_status

    await ensureConnected()

    app = web.Application()
    app.add_routes(routes)

    return app

if __name__ == '__main__':
    web.run_app(app_factory(), port=5678)
