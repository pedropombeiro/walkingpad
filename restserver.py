from aiohttp import web
from bleak.exc import BleakError
from ph4_walkingpad import pad
from ph4_walkingpad.pad import WalkingPad, Controller
from ph4_walkingpad.utils import setup_logging
from prometheus_client import start_http_server, Counter, Gauge, Enum
import asyncio
import time
import yaml
import psycopg2
from datetime import date, datetime

routes = web.RouteTableDef()
connectTask = None
current_request_ts = None
last_comm_rx_ts = 0
last_comm_tx_ts = 0
last_json_status_ts = 0
last_json_status = {}
session_start_steps = 0
session_start_dist = 0

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

prom_session_steps = Gauge('walkingpad_steps_session', 'WalkingPad session step count')
prom_steps_total = Counter('walkingpad_steps_total', 'WalkingPad total steps')
prom_session_distance = Gauge('walkingpad_distance_session', 'WalkingPad distance')
prom_distance_total = Counter('walkingpad_distance_total', 'WalkingPad total distance')
prom_speed = Gauge('walkingpad_speed', 'WalkingPad speed')
prom_state = Enum('walkingpad_state', 'WalkingPad state', states=['idle', 'standby', 'starting', 'running', 'stopping'])
prom_mode = Enum('walkingpad_mode', 'WalkingPad mode', states=['manual', 'auto', 'standby'])

def on_new_status(sender, record):
    global last_comm_rx_ts

    last_comm_rx_ts = time.time()

    distance_in_km = record.dist / 100
    print("Received Record:")
    print('Distance: {0}km'.format(distance_in_km))
    print('Time: {0} seconds'.format(record.time))
    print('Steps: {0}'.format(record.steps))

    last_status['steps'] = record.steps
    last_status['distance'] = distance_in_km
    last_status['time'] = record.time

def on_new_current_status(sender, record):
    global last_json_status, last_json_status_ts, last_comm_rx_ts

    last_comm_rx_ts = time.time()
    json_status = create_json_status(record)
    last_json_status_ts = time.time()
    last_json_status = json_status
    print("{0} - Received Current Record: {1}".format(datetime.now(), json_status))

    update_prom_status(json_status)

def on_web_request():
    global current_request_ts
    if current_request_ts is not None:
        raise web.HTTPTooManyRequests(text=str("Request in progress, please try again"))

    current_request_ts = time.time()

def update_prom_status(json_status):
    prom_session_steps.set(json_status["steps"])
    prom_session_distance.set(json_status["dist"])
    prom_speed.set(json_status["speed"])
    prom_state.state(json_status["belt_state"])
    prom_mode.state(json_status["mode"])

def create_json_status(status):
    mode = status.manual_mode
    belt_state = status.belt_state

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
    elif (belt_state == 3):
        belt_state = "stopping"
    elif (belt_state >=7):
        belt_state = "starting"

    dist = status.dist / 100
    time = status.time
    steps = status.steps
    speed = status.speed / 10

    return { "dist": dist, "time": time, "steps": steps, "speed": speed, "belt_state": belt_state, "mode": mode }


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
            log.warn(exc)


def save_config(config):
    with open('config.yaml', 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


async def ensureConnected(attempt=0):
    global connectTask, ctler
    global last_comm_rx_ts, last_comm_tx_ts

    if last_comm_tx_ts != 0 and (last_comm_tx_ts - last_comm_rx_ts) > 30:
        log.warning("Disconnecting after 30 seconds without connection")
        await disconnect()

    last_comm_tx_ts = time.time()

    log.debug("- Attempt #{0}".format(attempt+1))
    if connectTask is None:
        if (last_comm_tx_ts - last_comm_rx_ts) > 30:
            log.debug("- connectTask was None")
            connectTask = connect()
        else:
            # connectTask is None, but we're still valid
            return

    try:
        await connectTask
        connectTask = None
        last_comm_rx_ts = time.time()
        log.debug("- Setting connectTask to None")
    except BleakError as e:
        log.error("- BleakError in ensureConnected: {0}".format(e))

        if attempt < 1:
            await ctler.disconnect()

            connectTask = None
            await ensureConnected(attempt+1)
        else:
            log.debug("Giving up retrying. Raising '{0}'".format(e))
            connectTask = None
            last_comm_rx_ts = 0 # Force reconnection
            raise e
    except RuntimeError:
        # coroutine might be being awaited already, ignore
        pass
    except Exception as e:
        log.warning("- Unhandled exception: {0}".format(e))
        last_comm_rx_ts = 0 # Force reconnection
        raise e

async def connect():
    address = load_config()['address']
    log.info("Connecting to {0}".format(address))
    await ctler.run(address)
    await asyncio.sleep(minimal_cmd_space)


async def disconnect():
    log.debug("Disconnecting in restserver.py")

    global connectTask, ctler
    if connectTask is not None:
        try:
            await connectTask
        except RuntimeError:
            # ignore already awaited task
            pass
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
    if time.time() - last_json_status_ts < 5:
        return web.json_response({ "mode": last_json_status["mode"] })

    await ensureConnected()

    await ctler.ask_stats()
    await asyncio.sleep(minimal_cmd_space)
    stats = ctler.last_status
    if stats is None:
        return web.HTTPNotFound()

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
    global current_request_ts

    if request.rel_url.query.get('new_mode', '') != '':
        # Some clients, like the Stream Deck can only perform GET requests,
        # so we forward those to the POST handler
        return await change_pad_mode(request)

    on_web_request()

    try:
        return await _get_pad_mode()
    except BleakError as e:
        log.error("BleakError in get_pad_mode: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None

@routes.post("/mode")
async def change_pad_mode(request):
    global current_request_ts
    on_web_request()

    try:
        if request.body_exists:
            json_body = await request.json()
            new_mode = json_body['mode']
        else:
            new_mode = request.rel_url.query.get('new_mode', '')

        if (new_mode.lower() == "standby"):
            pad_mode = WalkingPad.MODE_STANDBY
        elif (new_mode.lower() == "manual"):
            pad_mode = WalkingPad.MODE_MANUAL
        elif (new_mode.lower() == "auto"):
            pad_mode = WalkingPad.MODE_AUTOMAT
        else:
            return web.HTTPBadRequest(text="Mode {0} not supported".format(new_mode))

        await ensureConnected()

        log.info("Switching mode to {0}".format(new_mode))
        await ctler.switch_mode(pad_mode)
        await asyncio.sleep(minimal_cmd_space)
        await ctler.switch_mode(pad_mode)
        await asyncio.sleep(minimal_cmd_space)

        return await _get_pad_mode()
    except BleakError as e:
        log.error("BleakError in get_pad_mode: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None


@routes.post("/speed")
async def change_speed(request):
    global current_request_ts
    on_web_request()

    try:
        if request.body_exists:
            json_body = await request.json()
            value = json_body['speed']
        else:
            value = request.rel_url.query.get('value', '')
        log.info("Setting speed to {0} km/h".format(value))

        await ctler.change_speed(int(float(value) * 10))

        await asyncio.sleep(minimal_cmd_space)
        await ctler.ask_stats()
        await asyncio.sleep(minimal_cmd_space)
        stats = ctler.last_status

        return web.json_response({ "speed": stats.app_speed / 30 })
    except BleakError as e:
        log.error("BleakError in change_speed: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None


@routes.post("/pref/{name}")
async def change_pref(request):
    global current_request_ts
    on_web_request()

    name = request.match_info.get('name', '')
    value = request.rel_url.query.get('value', '')
    log.info("Setting preference {0} to {1}".format(name, value))

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
        log.error("BleakError in change_pref: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None

    await asyncio.sleep(minimal_cmd_space)

@routes.get("/status")
async def get_status(request):
    global last_json_status, last_json_status_ts
    global current_request_ts

    if time.time() - last_json_status_ts < 5:
        json_status = last_json_status
    else:
        on_web_request()

        try:
            await ensureConnected()

            await ctler.ask_stats()
            await asyncio.sleep(minimal_cmd_space)
            json_status = last_json_status
        except BleakError as e:
            log.error("BleakError in get_status: {0}".format(e))
            await disconnect()
            raise web.HTTPServiceUnavailable(text=str(e))
        except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
            raise web.HTTPGone(text=str(e))
        finally:
            current_request_ts = None

    return web.json_response(json_status)

@routes.get("/history")
async def get_history(request):
    global current_request_ts
    on_web_request()

    try:
        await ensureConnected()

        await ctler.ask_hist(0)
        await asyncio.sleep(minimal_cmd_space)

        return web.json_response(last_status)
    except BleakError as e:
        log.error("BleakError in get_history: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None

@routes.post("/save")
def save(request):
    store_in_db(last_status['steps'], last_status['distance'], last_status['time'])

@routes.post("/startwalk")
async def start_walk(request):
    global session_start_steps, session_start_dist
    global current_request_ts

    on_web_request()

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

        session_start_steps = int(ctler.last_status.steps)
        session_start_dist = float(ctler.last_status.dist) / 100.0

        return web.json_response(last_status)
    except BleakError as e:
        log.error("BleakError in start_walk: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None

@routes.post("/finishwalk")
async def finish_walk(request):
    global current_request_ts
    while current_request_ts is not None:
        await asyncio.sleep(1)

    current_request_ts = time.time()

    try:
        await ensureConnected()

        await ctler.switch_mode(WalkingPad.MODE_STANDBY)
        await asyncio.sleep(minimal_cmd_space)

        await ctler.ask_stats()
        await asyncio.sleep(minimal_cmd_space)
        prom_steps_total.inc(int(ctler.last_status.steps) - session_start_steps)
        prom_distance_total.inc((float(ctler.last_status.dist) / 100.0) - session_start_dist)

        await ctler.ask_hist(0)
        await asyncio.sleep(minimal_cmd_space)
        store_in_db(last_status['steps'], last_status['distance'], last_status['time'])

        return web.json_response(last_status)
    except BleakError as e:
        log.error("BleakError in finish_walk: {0}".format(e))
        await disconnect()
        raise web.HTTPServiceUnavailable(text=str(e))
    except AttributeError as e: # AttributeError can leak from ph4_walkingpad if the device is not available on reconnection
        raise web.HTTPGone(text=str(e))
    finally:
        current_request_ts = None


async def app_factory():
    ctler.handler_last_status = on_new_status
    ctler.handler_cur_status = on_new_current_status

    try:
        await ensureConnected()
    except:
        pass

    app = web.Application()
    app.add_routes(routes)

    return app

if __name__ == '__main__':
    start_http_server(8000) # Start Prometheus server
    #logging.basicConfig(level=logging.INFO)
    web.run_app(app_factory(), port=5678, shutdown_timeout=15)
