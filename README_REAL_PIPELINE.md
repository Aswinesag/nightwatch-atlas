# Enhanced Intelligence Platform - Real Data Pipeline

## Overview
This redesigned intelligence platform eliminates all hardcoded data and uses real APIs for dynamic, real-time intelligence gathering.

## Architecture Changes

### 1. Data Sources
- **GDELT API**: Real global events database
- **NewsAPI**: Additional news sources (requires API key)
- **OpenStreetMap Nominatim**: Real geocoding service
- **No hardcoded fallback data**: All data comes from real APIs

### 2. Enhanced Pipeline Components

#### Harvester (`services/harvester-gdelt/main_real.py`)
- Real GDELT API integration
- Multiple data source support
- Proper error handling without fallback data
- Real-time polling with 30-second intervals

#### Processor (`services/processor-ai/gdelt_processor_real.py`)
- Real NLP-based entity extraction
- Actual geocoding using OpenStreetMap
- Dynamic risk calculation based on content
- Real location extraction from text

#### Database (`apps/control-plane/models_real.py`)
- Enhanced schema with location tracking
- Entity extraction storage
- Processing logs for debugging
- Data source management

#### Frontend (`apps/dashboard/src/App_real.tsx`)
- Dynamic event type detection
- Real-time WebSocket updates
- No hardcoded filter types
- Error handling and reconnection

#### API (`apps/control-plane/main_real.py`)
- Enhanced REST API with statistics
- Real-time event processing
- Health monitoring
- Dynamic filtering capabilities

## Key Improvements

### No Hardcoded Data
- Removed all fallback/test data
- Real API responses only
- Dynamic event type detection
- Actual geocoding services

### Enhanced Data Quality
- Real location extraction
- Content-based risk analysis
- Entity extraction and storage
- Processing audit logs

### Better Error Handling
- Graceful API failure handling
- WebSocket reconnection
- Comprehensive logging
- Health monitoring

### Dynamic Frontend
- Self-updating filter types
- Real-time event counts
- Error state handling
- Loading states

## Migration Steps

1. **Update Database Schema**
   ```bash
   python migrate_to_real.py
   ```

2. **Replace Services**
   ```bash
   # Stop old services
   # Start new real services
   ```

3. **Update Frontend**
   ```bash
   # Replace App.tsx with App_real.tsx
   # Add DynamicFilters component
   ```

## Configuration

### Environment Variables
```bash
# Optional: NewsAPI key for additional sources
NEWSAPI_KEY=your_api_key_here

# Optional: Custom geocoding service
GEOCODING_SERVICE=nominatim
```

### API Rate Limits
- GDELT: 1000 requests/hour
- NewsAPI: 1000 requests/hour (with key)
- Nominatim: 1 request/second

## Data Flow

1. **Harvester** → Real APIs → `raw.gdelt` topic
2. **Processor** → NLP/Geocoding → `events` topic  
3. **Control Plane** → Database + WebSocket → Dashboard
4. **Frontend** → Dynamic display → Real-time updates

## Monitoring

### Health Endpoints
- `GET /health` - System health status
- `GET /events/stats` - Real-time statistics
- `GET /events/types` - Available event types

### Logging
- Structured logging with levels
- Processing logs for debugging
- WebSocket connection monitoring
- API error tracking

## Performance

### Optimizations
- Database indexing on key fields
- WebSocket connection pooling
- Efficient event filtering
- Caching for repeated requests

### Scalability
- Horizontal scaling support
- Load balancing ready
- Database connection pooling
- Message queue buffering

## Security

### Data Protection
- No hardcoded credentials
- API key management
- Input validation
- Rate limiting

### Network Security
- CORS configuration
- WebSocket authentication (optional)
- API rate limiting
- Request validation

## Testing

### Unit Tests
- API endpoint testing
- Data processing validation
- Error handling verification

### Integration Tests
- End-to-end pipeline testing
- WebSocket connection testing
- Database migration testing

## Deployment

### Docker Configuration
Updated Docker Compose with real services:
- Enhanced harvester image
- Real processor service
- Updated control plane
- Frontend with dynamic components

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run database migration
python migrate_to_real.py

# Start services
docker-compose up -d

# Run enhanced services
python apps/control-plane/main_real.py
python services/processor-ai/gdelt_processor_real.py
```

## Troubleshooting

### Common Issues
1. **API Rate Limits**: Check service quotas
2. **Geocoding Failures**: Verify network connectivity
3. **WebSocket Issues**: Check firewall settings
4. **Database Errors**: Run migration script

### Debug Mode
Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python apps/control-plane/main_real.py
```

## Future Enhancements

### Additional Data Sources
- Twitter API integration
- Reddit API for social intelligence
- Government APIs for official data
- Satellite imagery analysis

### Advanced Features
- Machine learning for risk prediction
- Anomaly detection
- Predictive analytics
- Alert correlation

### Performance
- Redis caching layer
- Elasticsearch for search
- Microservices architecture
- GraphQL API

This redesigned platform provides a robust, scalable intelligence system with no hardcoded data, using real APIs for dynamic, real-time threat intelligence gathering.
