import requests
import json
import time
import uuid
import os
import logging
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment variables
API_KEY = os.getenv("OTX_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

if not API_KEY:
    logger.error("OTX_KEY environment variable not set")
    exit(1)

# Initialize Kafka producer with explicit configuration
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        client_id='otx-harvester',
        request_timeout_ms=10000,
        api_version=(0, 10, 1),
        max_block_ms=5000,
        retries=3,
        retry_backoff_ms=100
    )
    logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    exit(1)

# OTX API endpoints
URL_PULSES = "https://otx.alienvault.com/api/v1/pulses/subscribed"
URL_PULSE_DETAILS = "https://otx.alienvault.com/api/v1/pulses"
URL_INDICATORS = "https://otx.alienvault.com/api/v1/indicators"

def fetch_recent_pulses():
    """Fetch recent cyber threat pulses from OTX"""
    try:
        headers = {
            "X-OTX-API-KEY": API_KEY,
            "Content-Type": "application/json"
        }
        
        # Get pulses from the last 24 hours
        params = {
            "limit": 50,
            "modified_since": (datetime.utcnow() - timedelta(hours=24)).isoformat(),
            "sort": "-modified"
        }
        
        response = requests.get(URL_PULSES, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if "results" not in data:
            logger.error("Invalid OTX response format")
            return None
        
        pulses = data.get("results", [])
        logger.info(f"Fetched {len(pulses)} cyber threat pulses from OTX")
        
        # Filter for high-priority threats
        high_priority_pulses = []
        for pulse in pulses:
            if is_high_priority_threat(pulse):
                high_priority_pulses.append(pulse)
        
        logger.info(f"Found {len(high_priority_pulses)} high-priority cyber threats")
        return high_priority_pulses
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching OTX data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

def is_high_priority_threat(pulse):
    """Determine if threat is high priority for intelligence monitoring"""
    try:
        # Get threat indicators
        tags = pulse.get("tags", [])
        description = pulse.get("description", "").lower()
        name = pulse.get("name", "").lower()
        
        # High-priority threat indicators
        high_priority_tags = [
            "apt", "malware", "ransomware", "trojan", "backdoor",
            "exploit", "vulnerability", "zero-day", "phishing",
            "botnet", "ddos", "data breach", "leak"
        ]
        
        # Check for high-priority tags
        if any(tag.lower() in high_priority_tags for tag in tags):
            return True
        
        # Check for high-priority keywords in description
        high_priority_keywords = [
            "critical", "severe", "urgent", "active", "ongoing",
            "military", "government", "infrastructure", "energy",
            "financial", "healthcare", "critical infrastructure"
        ]
        
        combined_text = f"{name} {description}"
        if any(keyword in combined_text for keyword in high_priority_keywords):
            return True
        
        # Check for recent activity (modified in last 6 hours)
        modified = pulse.get("modified", "")
        if modified:
            try:
                modified_time = datetime.fromisoformat(modified.replace('Z', '+00:00'))
                if (datetime.utcnow() - modified_time.replace(tzinfo=None)) < timedelta(hours=6):
                    return True
            except:
                pass
        
        # Check for high number of indicators
        if pulse.get("indicator_count", 0) > 10:
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error evaluating threat priority: {e}")
        return False

def extract_threat_intelligence(pulse):
    """Extract detailed threat intelligence from pulse data"""
    try:
        # Basic pulse information
        name = pulse.get("name", "Unknown Threat")
        description = pulse.get("description", "No description available")
        tags = pulse.get("tags", [])
        
        # Extract indicators
        indicators = pulse.get("indicators", [])
        
        # Analyze threat type
        threat_type = determine_threat_type(tags, description, indicators)
        
        # Calculate risk
        risk = calculate_threat_risk(pulse)
        
        # Extract geographic information
        location = extract_threat_location(pulse)
        
        # Extract affected sectors
        affected_sectors = extract_affected_sectors(pulse)
        
        # Extract attack patterns
        attack_patterns = extract_attack_patterns(indicators)
        
        return {
            "name": name,
            "description": description,
            "threat_type": threat_type,
            "risk": risk,
            "location": location,
            "affected_sectors": affected_sectors,
            "attack_patterns": attack_patterns,
            "indicator_count": len(indicators),
            "tags": tags,
            "pulse_id": pulse.get("id"),
            "author_name": pulse.get("author_name"),
            "created": pulse.get("created"),
            "modified": pulse.get("modified")
        }
        
    except Exception as e:
        logger.error(f"Error extracting threat intelligence: {e}")
        return None

def determine_threat_type(tags, description, indicators):
    """Determine threat type from pulse data"""
    tags_lower = [tag.lower() for tag in tags]
    desc_lower = description.lower()
    
    # Military/State-sponsored threats
    military_indicators = ["apt", "state-sponsored", "nation-state", "military", "government"]
    if any(indicator in tags_lower or indicator in desc_lower for indicator in military_indicators):
        return "political"
    
    # Infrastructure attacks
    infrastructure_indicators = ["ics", "scada", "industrial", "critical infrastructure", "energy"]
    if any(indicator in tags_lower or indicator in desc_lower for indicator in infrastructure_indicators):
        return "infrastructure"
    
    # Malware/Ransomware
    malware_indicators = ["malware", "ransomware", "trojan", "backdoor", "virus", "worm"]
    if any(indicator in tags_lower or indicator in desc_lower for indicator in malware_indicators):
        return "military"
    
    # Data breaches/Leaks
    breach_indicators = ["breach", "leak", "exposure", "data", "privacy"]
    if any(indicator in tags_lower or indicator in desc_lower for indicator in breach_indicators):
        return "general"
    
    return "general"

def calculate_threat_risk(pulse):
    """Calculate threat risk based on multiple factors"""
    risk = 0.3  # Base risk
    
    # Tags analysis
    tags = pulse.get("tags", [])
    high_risk_tags = ["apt", "zero-day", "critical", "severe", "active", "ongoing"]
    if any(tag.lower() in high_risk_tags for tag in tags):
        risk += 0.3
    
    # Indicator count
    indicator_count = pulse.get("indicator_count", 0)
    if indicator_count > 50:
        risk += 0.2
    elif indicator_count > 20:
        risk += 0.1
    
    # Recency
    modified = pulse.get("modified", "")
    if modified:
        try:
            modified_time = datetime.fromisoformat(modified.replace('Z', '+00:00'))
            hours_ago = (datetime.utcnow() - modified_time.replace(tzinfo=None)).total_seconds() / 3600
            if hours_ago < 6:
                risk += 0.2
            elif hours_ago < 24:
                risk += 0.1
        except:
            pass
    
    # Author reputation (assuming some authors are more reliable)
    author = pulse.get("author_name", "").lower()
    if any(reliable in author for reliable in ["alienvault", "fireeye", "mandiant", "crowdstrike"]):
        risk += 0.1
    
    return min(0.95, risk)

def extract_threat_location(pulse):
    """Extract geographic information from threat data"""
    try:
        description = pulse.get("description", "").lower()
        tags = [tag.lower() for tag in pulse.get("tags", [])]
        
        # Country mentions
        countries = [
            "united states", "china", "russia", "north korea", "iran",
            "israel", "ukraine", "saudi arabia", "india", "pakistan",
            "united kingdom", "germany", "france", "japan", "south korea"
        ]
        
        mentioned_countries = []
        for country in countries:
            if country in description or country in " ".join(tags):
                mentioned_countries.append(country)
        
        if mentioned_countries:
            # Return first mentioned country with approximate coordinates
            country_coords = {
                "united states": {"lat": 39.8283, "lon": -98.5795},
                "china": {"lat": 35.8617, "lon": 104.1954},
                "russia": {"lat": 61.5240, "lon": 105.3188},
                "north korea": {"lat": 40.3399, "lon": 127.5101},
                "iran": {"lat": 32.4279, "lon": 53.6880},
                "israel": {"lat": 31.0461, "lon": 34.8516},
                "ukraine": {"lat": 48.3794, "lon": 31.1656},
                "saudi arabia": {"lat": 23.8859, "lon": 45.0792},
                "india": {"lat": 20.5937, "lon": 78.9629},
                "pakistan": {"lat": 30.3753, "lon": 69.3451},
                "united kingdom": {"lat": 55.3781, "lon": -3.4360},
                "germany": {"lat": 51.1657, "lon": 10.4515},
                "france": {"lat": 46.2276, "lon": 2.2137},
                "japan": {"lat": 36.2048, "lon": 138.2529},
                "south korea": {"lat": 35.9078, "lon": 127.7669}
            }
            
            country = mentioned_countries[0]
            if country in country_coords:
                coords = country_coords[country]
                return {
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "display_name": country.title(),
                    "country": country.title(),
                    "confidence": 0.6
                }
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting threat location: {e}")
        return None

def extract_affected_sectors(pulse):
    """Extract affected sectors from threat data"""
    try:
        description = pulse.get("description", "").lower()
        tags = [tag.lower() for tag in pulse.get("tags", [])]
        
        sectors = []
        
        # Sector keywords
        sector_keywords = {
            "financial": ["bank", "financial", "payment", "credit card", "atm"],
            "healthcare": ["hospital", "medical", "healthcare", "pharmaceutical"],
            "energy": ["energy", "power", "electric", "oil", "gas", "nuclear"],
            "government": ["government", "military", "defense", "federal", "state"],
            "technology": ["software", "hardware", "tech", "cloud", "data"],
            "telecommunications": ["telecom", "communication", "network", "internet"]
        }
        
        for sector, keywords in sector_keywords.items():
            if any(keyword in description or keyword in " ".join(tags) for keyword in keywords):
                sectors.append(sector)
        
        return sectors
        
    except Exception as e:
        logger.error(f"Error extracting affected sectors: {e}")
        return []

def extract_attack_patterns(indicators):
    """Extract attack patterns from indicators"""
    try:
        patterns = []
        
        for indicator in indicators[:10]:  # Limit to first 10 indicators
            indicator_type = indicator.get("type", "").lower()
            indicator_value = indicator.get("indicator", "").lower()
            
            # Common attack patterns
            if "domain" in indicator_type:
                if "malware" in indicator_value or "c2" in indicator_value:
                    patterns.append("c2_communication")
                elif "phishing" in indicator_value:
                    patterns.append("phishing")
                else:
                    patterns.append("malicious_domain")
            
            elif "ip" in indicator_type:
                patterns.append("malicious_ip")
            
            elif "url" in indicator_type:
                patterns.append("malicious_url")
            
            elif "file_hash" in indicator_type:
                patterns.append("malware")
        
        return list(set(patterns))  # Remove duplicates
        
    except Exception as e:
        logger.error(f"Error extracting attack patterns: {e}")
        return []

def convert_to_threat_event(pulse):
    """Convert OTX pulse to standardized event format"""
    try:
        # Extract threat intelligence
        threat_intel = extract_threat_intelligence(pulse)
        if not threat_intel:
            return None
        
        # Determine severity based on risk
        risk = threat_intel["risk"]
        if risk >= 0.8:
            severity = "critical"
        elif risk >= 0.6:
            severity = "high"
        elif risk >= 0.4:
            severity = "medium"
        else:
            severity = "low"
        
        # Get location data
        location = threat_intel.get("location")
        
        event = {
            "id": str(uuid.uuid4()),
            "title": threat_intel["name"],
            "description": threat_intel["description"],
            "source": "OTX",
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": location["lat"] if location else None,
            "longitude": location["lon"] if location else None,
            "risk": risk,
            "type": threat_intel["threat_type"],
            "severity": severity,
            "location_info": location,
            "threat_intelligence": {
                "indicator_count": threat_intel["indicator_count"],
                "affected_sectors": threat_intel["affected_sectors"],
                "attack_patterns": threat_intel["attack_patterns"],
                "tags": threat_intel["tags"],
                "pulse_id": threat_intel["pulse_id"],
                "author": threat_intel["author_name"],
                "created": threat_intel["created"],
                "modified": threat_intel["modified"]
            },
            "processed_at": datetime.utcnow().isoformat()
        }
        
        return event
        
    except Exception as e:
        logger.error(f"Error converting threat data: {e}")
        return None

def run():
    """Main harvester loop"""
    logger.info("Starting OTX real-time cyber threat harvester...")
    logger.info(f"API Key: {API_KEY[:10]}..." if API_KEY else "No API Key")
    logger.info(f"Polling interval: 10 minutes")
    
    message_count = 0
    
    while True:
        try:
            logger.info("Fetching recent cyber threat pulses...")
            pulses = fetch_recent_pulses()
            
            if pulses:
                processed_count = 0
                for pulse in pulses[:15]:  # Limit to top 15 threats
                    event = convert_to_threat_event(pulse)
                    if event:
                        producer.send("raw.otx", event)
                        processed_count += 1
                        message_count += 1
                        logger.info(f"Threat: {event['title'][:50]}... - Risk: {event['risk']:.2f}")
                
                producer.flush()
                logger.info(f"Sent {processed_count} cyber threat events to Kafka (Total: {message_count})")
                
            else:
                logger.warning("No cyber threat data received from OTX")
                
        except Exception as e:
            logger.error(f"Error in OTX harvester loop: {e}")
        
        # OTX updates less frequently than aircraft tracking
        time.sleep(600)  # 10 minutes

if __name__ == "__main__":
    if not API_KEY:
        logger.error("OTX API key not found in environment variables")
        logger.error("Please set OTX_KEY in your .env file")
        import sys
        sys.exit(1)
    
    run()
