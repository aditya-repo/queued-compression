import pymongo
import logging
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
# Setup basic logging
logging.basicConfig(level=logging.INFO)

class MongoDBOperations:
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def check_if_processing(self):
        """Fetch clientId based on specific conditions."""
        data = self.collection.find_one(
            {
                "status": {"$in": ["processing"]}
            },
            {
                "clientId": 1,  # Only include the clientId field in the result
                "_id": 0        # Exclude the default _id field from the result
            }
        )

        return data  
    
    def fetch_data(self):
        """Fetch clientId based on specific conditions."""
        data = self.collection.find_one(
            {
                "status": {"$in": ["queued"]}
            },
            {
                "clientId": 1,  # Only include the clientId field in the result
                "_id": 0        # Exclude the default _id field from the result
            }
        )

        return data  

    def update_status(self, client_code, total_files, processed_files):
        """Update the status and file counts in the database."""
        result = self.collection.update_one(
            {"clientId": client_code},  # Ensure to use clientId for matching
            {
                "$set": {
                    "totalfile": total_files,
                    "status": "processing",
                    "processedfile": processed_files
                }
            }
        )
        if result.modified_count == 0:
            logging.warning(f"No documents were updated for clientId {client_code}.")

    def finalize_processing(self, client_code, total_processed):
        """Finalize processing and update completion status."""
        result = self.collection.update_one(
            {"clientId": client_code},  # Ensure to use clientId for matching
            {
                "$set": {
                    "processedfile": total_processed,
                    "status": "cdn-queued"
                }
            }
        )
        
        if result.modified_count == 0:
            logging.warning(f"No documents were finalized for clientId {client_code}.")
