import requests
import json
import time
import uuid
import os
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

USERNAME = os.getenv("OPENSKY_USER")
PASSWORD = os.getenv("OPENSKY_PASS")
API_KEY = os.getenv("OPENSKY_API_KEY")
OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
OPENSKY_CREDENTIALS_FILE = os.getenv("OPENSKY_CREDENTIALS_FILE", "/app/credentials.json")
POLL_INTERVAL_SECONDS = int(os.getenv("OPENSKY_POLL_INTERVAL", "15"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("OPENSKY_REQUEST_TIMEOUT", "12"))

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "intel-platform-opensky-harvester/1.0",
        "Accept": "application/json",
    }
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# OpenSky API endpoints
URL_ALL_STATES = "https://opensky-network.org/api/states/all"
URL_OWNERSHIP = "https://opensky-network.org/api/aircraft/ownership/"
URL_OAUTH_TOKEN = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

_oauth_access_token: Optional[str] = None
_oauth_expires_at_epoch: float = 0.0

def _load_oauth_credentials() -> Tuple[Optional[str], Optional[str]]:
    """Load OAuth client credentials from env first, then credentials.json."""
    if OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET:
        return OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET

    try:
        with open(OPENSKY_CREDENTIALS_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
            cid = payload.get("clientId") or payload.get("client_id")
            csecret = payload.get("clientSecret") or payload.get("client_secret")
            return cid, csecret
    except FileNotFoundError:
        return None, None
    except Exception as exc:
        logger.warning(f"Failed to parse OpenSky credentials file: {exc}")
        return None, None

def _get_oauth_access_token(force_refresh: bool = False) -> Optional[str]:
    """Fetch/cached OAuth access token using client credentials flow."""
    global _oauth_access_token, _oauth_expires_at_epoch

    now = time.time()
    # Refresh 60 seconds early to avoid edge expiry during request.
    if not force_refresh and _oauth_access_token and now < (_oauth_expires_at_epoch - 60):
        return _oauth_access_token

    client_id, client_secret = _load_oauth_credentials()
    if not client_id or not client_secret:
        return None

    try:
        response = session.post(
            URL_OAUTH_TOKEN,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 300))
        if not token:
            logger.warning("OpenSky OAuth token response missing access_token")
            return None
        _oauth_access_token = token
        _oauth_expires_at_epoch = now + max(60, expires_in)
        return _oauth_access_token
    except requests.exceptions.RequestException as exc:
        logger.warning(f"OpenSky OAuth token request failed: {exc}")
        return None

def _extract_retry_after_seconds(response: requests.Response) -> int:
    """Read OpenSky retry headers and return seconds to wait."""
    retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds") or response.headers.get("Retry-After")
    if not retry_after:
        return 0
    try:
        return max(0, int(retry_after))
    except ValueError:
        return 0

def fetch_states_with_fallback(params: dict) -> Tuple[Optional[requests.Response], str, int]:
    """
    Try multiple auth modes in order:
    1) X-API-Key (if present)
    2) Basic auth (if username/password present)
    3) Anonymous
    """
    attempts = []
    oauth_token = _get_oauth_access_token()
    if oauth_token:
        attempts.append(("oauth_bearer", {"Authorization": f"Bearer {oauth_token}"}, None))
    if API_KEY:
        attempts.append(("api_key", {"X-API-Key": API_KEY}, None))
    if USERNAME and PASSWORD:
        attempts.append(("basic", {}, (USERNAME, PASSWORD)))
    attempts.append(("anonymous", {}, None))

    last_error = None
    max_retry_after_seconds = 0
    for mode, extra_headers, auth in attempts:
        try:
            response = session.get(
                URL_ALL_STATES,
                params=params,
                auth=auth,
                headers=extra_headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.warning(f"OpenSky request failed in {mode} mode: {exc}")
            continue

        if response.status_code == 200:
            return response, mode, 0

        # Keep trying next mode for expected auth/rate-limit failures.
        if response.status_code in (401, 403, 429):
            if mode == "oauth_bearer" and response.status_code == 401:
                refreshed = _get_oauth_access_token(force_refresh=True)
                if refreshed and refreshed != oauth_token:
                    logger.warning("OpenSky OAuth token expired/invalid; retrying with refreshed token")
                    retry_response = session.get(
                        URL_ALL_STATES,
                        params=params,
                        headers={"Authorization": f"Bearer {refreshed}"},
                        timeout=REQUEST_TIMEOUT_SECONDS,
                    )
                    if retry_response.status_code == 200:
                        return retry_response, "oauth_bearer_refreshed", 0
                    response = retry_response
            retry_after_seconds = _extract_retry_after_seconds(response)
            if retry_after_seconds:
                max_retry_after_seconds = max(max_retry_after_seconds, retry_after_seconds)
            logger.warning(f"OpenSky {mode} mode returned HTTP {response.status_code}")
            continue

        # For other response codes, treat as hard failure.
        response.raise_for_status()

    if last_error:
        raise last_error
    return None, "none", max_retry_after_seconds

def fetch_aircraft_data() -> Tuple[Optional[list], int]:
    """Fetch real-time aircraft data from OpenSky API"""
    try:
        # Get all aircraft states
        params = {
            "icao24": None,  # All aircraft
            "lamin": 0,  # No latitude filter
            "lamax": 90,  # Worldwide
            "lomin": -180,  # Worldwide longitude
            "lomax": 180
        }

        response, auth_mode, retry_after_seconds = fetch_states_with_fallback(params)
        if response is None:
            logger.warning("OpenSky request failed for all auth modes")
            return None, retry_after_seconds

        logger.info(f"OpenSky request succeeded via {auth_mode} mode")
        data = response.json()
        
        # Validate response
        if "states" not in data:
            logger.error("Invalid OpenSky response format")
            return None
        
        states = data.get("states", [])
        logger.info(f"Fetched {len(states)} aircraft states from OpenSky")
        
        # Filter for interesting aircraft (military, high altitude, unusual patterns)
        interesting_aircraft = []
        for state in states:
            if is_interesting_aircraft(state):
                interesting_aircraft.append(state)
        
        logger.info(f"Found {len(interesting_aircraft)} interesting aircraft")

        if interesting_aircraft:
            return interesting_aircraft, 0

        # If no aircraft pass filters, still publish a geolocated sample of live flights.
        geolocated_states = [
            s for s in states
            if len(s) > 6 and s[5] is not None and s[6] is not None and not (len(s) > 8 and s[8] is True)
        ]
        fallback = geolocated_states[:20]
        logger.info(f"No interesting aircraft found; using {len(fallback)} geolocated fallback states")
        return fallback, 0
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching OpenSky data: {e}")
        return None, 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None, 0

def is_interesting_aircraft(state):
    """Determine if aircraft is interesting for intelligence monitoring"""
    try:
        # Extract aircraft data
        callsign = state[1] if len(state) > 1 else ""
        altitude = state[7] if len(state) > 7 else 0
        velocity = state[9] if len(state) > 9 else 0
        on_ground = state[8] if len(state) > 8 else True
        
        # Skip aircraft on ground
        if on_ground:
            return False
        
        # Military aircraft patterns
        military_patterns = [
            "MIL", "AF", "NAVY", "ARMY", "USAF", "USN", "USMC",
            "RAF", "FIGHTER", "BOMBER", "TRANSPORT", "HELICOPTER",
            "C130", "C17", "F16", "F18", "F22", "F35", "A10",
            "B52", "B1", "B2", "KC135", "KC46", "E3", "AWACS"
        ]
        
        if callsign and any(pattern in callsign.upper() for pattern in military_patterns):
            return True
        
        # High altitude aircraft (potential surveillance/military)
        if altitude and altitude > 40000:  # Above 40,000 feet
            return True
        
        # High speed aircraft
        if velocity and velocity > 600:  # Above 600 m/s (~Mach 1.7)
            return True
        
        # Aircraft with unusual patterns (hovering, circling)
        if velocity and velocity < 50 and altitude and altitude > 10000:
            return True  # Slow flying at high altitude (surveillance)
        
        # Commercial aircraft in conflict regions (for monitoring)
        if callsign and any(airline in callsign.upper() for airline in ["UAL", "DAL", "AAL"]):
            # Could add geographic filtering for conflict zones
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error evaluating aircraft: {e}")
        return False

def convert_to_aircraft_event(state):
    """Convert OpenSky state to standardized event format"""
    try:
        # Extract aircraft data from OpenSky state array
        icao24 = state[0] if len(state) > 0 else ""
        callsign = state[1] if len(state) > 1 else ""
        last_contact = state[2] if len(state) > 2 else 0
        longitude = state[5] if len(state) > 5 else None
        latitude = state[6] if len(state) > 6 else None
        altitude = state[7] if len(state) > 7 else 0
        on_ground = state[8] if len(state) > 8 else True
        velocity = state[9] if len(state) > 9 else 0
        heading = state[10] if len(state) > 10 else 0
        vertical_rate = state[11] if len(state) > 11 else 0
        sensors = state[12] if len(state) > 12 else []
        geo_altitude = state[13] if len(state) > 13 else 0
        squawk = state[14] if len(state) > 14 else ""
        spi = state[15] if len(state) > 15 else False
        position_source = state[16] if len(state) > 16 else 0
        
        # Calculate risk based on aircraft characteristics
        risk = calculate_aircraft_risk(callsign, altitude, velocity, squawk)
        
        # Determine event type
        event_type = determine_aircraft_type(callsign, altitude, velocity)
        
        # Determine severity
        if risk >= 0.8:
            severity = "critical"
        elif risk >= 0.6:
            severity = "high"
        elif risk >= 0.4:
            severity = "medium"
        else:
            severity = "low"
        
        event = {
            "id": str(uuid.uuid4()),
            "title": f"Aircraft {callsign or 'Unknown'} detected",
            "description": f"Aircraft ICAO24: {icao24}, Callsign: {callsign or 'Unknown'}, Altitude: {altitude}m, Velocity: {velocity}m/s, Heading: {heading}°",
            "source": "OpenSky",
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": latitude,
            "longitude": longitude,
            "risk": risk,
            "type": event_type,
            "severity": severity,
            "aircraft_data": {
                "icao24": icao24,
                "callsign": callsign,
                "altitude": altitude,
                "velocity": velocity,
                "heading": heading,
                "vertical_rate": vertical_rate,
                "squawk": squawk,
                "on_ground": on_ground,
                "last_contact": last_contact,
                "position_source": position_source
            },
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error converting aircraft data: {e}")
        return None

def calculate_aircraft_risk(callsign, altitude, velocity, squawk):
    """Calculate risk based on aircraft characteristics"""
    risk = 0.2  # Base risk
    
    # Military aircraft
    if callsign and any(pattern in callsign.upper() for pattern in ["MIL", "AF", "NAVY", "ARMY"]):
        risk += 0.4
    
    # High altitude
    if altitude and altitude > 50000:
        risk += 0.3
    elif altitude and altitude > 40000:
        risk += 0.2
    
    # High speed
    if velocity and velocity > 800:
        risk += 0.3
    elif velocity and velocity > 600:
        risk += 0.2
    
    # Emergency squawk codes
    if squawk in ["7700", "7500", "7600"]:
        risk += 0.5
    
    # No callsign (suspicious)
    if not callsign:
        risk += 0.2
    
    return min(0.95, risk)

def determine_aircraft_type(callsign, altitude, velocity):
    """Determine aircraft type based on characteristics"""
    if callsign:
        callsign_upper = callsign.upper()
        
        # Military patterns
        if any(pattern in callsign_upper for pattern in ["MIL", "AF", "NAVY", "ARMY", "USAF", "USN", "USMC", "RAF"]):
            return "military"
        
        # Commercial patterns
        if any(pattern in callsign_upper for pattern in ["UAL", "DAL", "AAL", "SWA", "LUF", "BA", "LH", "AF"]):
            return "infrastructure"
    
    # Based on altitude and velocity
    if altitude and altitude > 40000:
        return "military"  # High altitude likely military
    elif velocity and velocity > 800:
        return "military"  # High speed likely military
    
    return "infrastructure"

def run():
    """Main harvester loop"""
    logger.info("Starting OpenSky real-time aircraft harvester...")
    client_id, client_secret = _load_oauth_credentials()
    logger.info(
        "Auth config: oauth_client=%s, api_key=%s, userpass=%s",
        "set" if client_id and client_secret else "missing",
        "set" if API_KEY else "missing",
        "set" if USERNAME and PASSWORD else "missing",
    )
    logger.info(f"Polling interval: {POLL_INTERVAL_SECONDS} seconds")
    
    message_count = 0
    
    while True:
        sleep_seconds = POLL_INTERVAL_SECONDS
        try:
            logger.info("Fetching real-time aircraft data...")
            aircraft_states, retry_after_seconds = fetch_aircraft_data()
            
            if aircraft_states:
                processed_count = 0
                for state in aircraft_states[:20]:  # Limit to top 20 interesting aircraft
                    event = convert_to_aircraft_event(state)
                    if event:
                        producer.send("raw.opensky", event)
                        processed_count += 1
                        message_count += 1
                        logger.info(f"Aircraft {event['aircraft_data']['callsign']} - Risk: {event['risk']:.2f}")
                
                producer.flush()
                logger.info(f"Sent {processed_count} aircraft events to Kafka (Total: {message_count})")
                
            else:
                logger.warning("No aircraft data received from OpenSky")
                if retry_after_seconds > 0:
                    sleep_seconds = max(POLL_INTERVAL_SECONDS, retry_after_seconds)
                    logger.warning(f"Rate-limited by OpenSky; backing off for {sleep_seconds} seconds")
                
        except Exception as e:
            logger.error(f"Error in OpenSky harvester loop: {e}")
        
        # Respect OpenSky rate limits, especially for anonymous fallback.
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    cid, csecret = _load_oauth_credentials()
    if not (cid and csecret) and not API_KEY and (not USERNAME or not PASSWORD):
        logger.error("OpenSky credentials not found in environment variables")
        logger.error("Set OAuth client credentials (credentials.json or env), OPENSKY_API_KEY, or OPENSKY_USER/OPENSKY_PASS")
        import sys
        sys.exit(1)
    
    run()
