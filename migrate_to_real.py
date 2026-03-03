"""
Migration script to transition from hardcoded to real data pipeline
"""
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.control_plane.db.database import engine, Base
from apps.control_plane.models_real import Event, EventEntity, EventLocation, DataSource, ProcessingLog

def migrate_database():
    """Migrate database to new schema"""
    print("Starting database migration...")
    
    # Create new tables
    Base.metadata.create_all(bind=engine)
    
    # Insert data sources
    from sqlalchemy.orm import SessionLocal
    db = SessionLocal()
    
    try:
        # Check if data sources already exist
        existing_sources = db.query(DataSource).all()
        if not existing_sources:
            # Add default data sources
            data_sources = [
                DataSource(name="GDELT", type="api", url="https://api.gdeltproject.org/api/v2/doc/doc", 
                           api_key_required=False, rate_limit=1000),
                DataSource(name="NewsAPI", type="api", url="https://newsapi.org/v2/everything", 
                           api_key_required=True, rate_limit=1000),
                DataSource(name="Manual", type="manual", url="", 
                           api_key_required=False, rate_limit=0),
            ]
            
            for source in data_sources:
                db.add(source)
            
            db.commit()
            print("Data sources added successfully")
        
        print("Database migration completed successfully")
        
    except Exception as e:
        print(f"Migration error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    migrate_database()
