#!/usr/bin/env python3

print("Testing basic imports...")

try:
    print("1. Testing sys.path...")
    import sys
    print(f"   sys.path[0]: {sys.path[0]}")
    
    print("2. Testing db.database import...")
    from db.database import engine, Base, SessionLocal
    print("   db.database imported successfully")
    
    print("3. Testing models import...")
    from models import Event
    print("   Models imported successfully")
    
    print("4. Testing schemas import...")
    from schemas import EventCreate
    print("   Schemas imported successfully")
    
    print("5. Testing FastAPI...")
    from fastapi import FastAPI, Depends
    from sqlalchemy.orm import Session
    print("   FastAPI imported successfully")
    
    print("6. Creating app...")
    app = FastAPI()
    print("   App created successfully")
    
    print("7. Testing table creation...")
    Base.metadata.create_all(bind=engine)
    print("   Tables created successfully")
    
    print("8. Setting up get_db function...")
    from sqlalchemy.orm import sessionmaker
    
    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    print("   get_db function created successfully")
    
    print("9. Testing POST endpoint...")
    @app.post('/events')
    def create_event(event_data: dict, db: Session = Depends(get_db)):
        new_event = Event(
            id=event_data.get('id'),
            title=event_data.get('title'),
            description=event_data.get('description'),
            risk=event_data.get('risk'),
            lat=event_data.get('lat'),
            lon=event_data.get('lon'),
            type=event_data.get('type'),
            severity=event_data.get('severity'),
            created_at=event_data.get('created_at')
        )
        db.add(new_event)
        db.commit()
        return new_event
    
    print("10. All tests passed!")
    
except Exception as e:
    print(f"Error: {e}")

print("Test completed!")
