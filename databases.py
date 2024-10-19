import pymongo
import logging
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('SECRET_KEY')
# Setup basic logging
logging.basicConfig(level=logging.INFO)

class MongoDBOperations:
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def fetch_data(self, client_id):
        """Fetch data based on specific conditions."""
        data = self.collection.find(
            {
                "clientId": client_id,
                "status": {"$in": ["queued", "processing"]}  # Fetch where status is queued or processing
            }
        ).sort("queuedtime", pymongo.ASCENDING)  # Sort by queuedtime ascending
        return list(data)  # Return as a list

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
                    "status": "completed"
                }
            }
        )
        if result.modified_count == 0:
            logging.warning(f"No documents were finalized for clientId {client_code}.")
