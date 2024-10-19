import os
import cv2
import logging
import concurrent.futures
import time
from databases import MongoDBOperations  # Import your MongoDB operations class

# Setup basic logging
logging.basicConfig(level=logging.INFO)

# Function to update progress every 100 files
def update_progress(processed_files, total_files):
    """Function to print progress every 100 files."""
    progress = int((processed_files / total_files) * 100)
    logging.info(f"Processed {processed_files}/{total_files} files ({progress}% complete).")

# Function to call when all files are processed
def processing_completed(client_code, total_processed, db_operations):
    """Function to perform task after processing is complete."""
    db_operations.finalize_processing(client_code, total_processed)
    logging.info("All files have been processed successfully and database updated.")

def resize_and_save_image(image, output_path, width):
    """Resize and save the image to the output path."""
    ratio = width / image.shape[1]
    height = int(image.shape[0] * ratio)
    resized_image = cv2.resize(image, (width, height))
    cv2.imwrite(output_path, resized_image)

def process_single_image(image_path, final_image_path, thumbnail_image_path):
    """Process a single image by resizing and saving final and thumbnail versions."""
    try:
        image = cv2.imread(image_path)
        if image is None:
            logging.warning(f"Unable to read image {os.path.basename(image_path)}. Skipping.")
            return False
        
        # Save the final and thumbnail versions
        resize_and_save_image(image, final_image_path, 1800)
        resize_and_save_image(image, thumbnail_image_path, 240)
        return True

    except Exception as e:
        logging.error(f"Error processing {os.path.basename(image_path)}: {e}")
        return False

def process_images_in_directory(folder_path, final_folder_base, thumbnail_folder_base, total_files, processed_files_ref, db_operations, client_code):
    """Process images in the given directory and save outputs in the specified structure."""
    client_id = os.path.basename(folder_path)  # Extract folder name as client id
    final_folder = os.path.join(final_folder_base, client_id)
    thumbnail_folder = os.path.join(thumbnail_folder_base, client_id)
    
    # Create subfolders for the client inside final and thumbnail folders
    os.makedirs(final_folder, exist_ok=True)
    os.makedirs(thumbnail_folder, exist_ok=True)
    
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]

    if not files:
        logging.info(f"No files to process in {folder_path}.")
        return

    # Use ThreadPoolExecutor for multithreading
    num_workers = 3  # Number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for index, filename in enumerate(files):
            image_path = os.path.join(folder_path, filename)
            final_image_path = os.path.join(final_folder, filename)
            thumbnail_image_path = os.path.join(thumbnail_folder, filename)
            
            future = executor.submit(process_single_image, image_path, final_image_path, thumbnail_image_path)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                processed_files_ref[0] += 1  # Update total processed files count
                # Every 100 files, call the progress function and update the database
                if processed_files_ref[0] % 200 == 0:
                    update_progress(processed_files_ref[0], total_files)
                    db_operations.update_status(client_code, total_files, processed_files_ref[0])
            else:
                logging.error(f"Failed to process file in {folder_path}")

    logging.info(f"Completed processing in {folder_path}.")

def process_multiple_folders(input_base_path, output_base_path, client_code, db_operations):
    """Process multiple folders from the input base path and create final/thumbnail structure."""
    
    # Create top-level final and thumbnail folders
    final_folder_base = os.path.join(output_base_path, 'final')
    thumbnail_folder_base = os.path.join(output_base_path, 'thumbnail')
    
    os.makedirs(final_folder_base, exist_ok=True)
    os.makedirs(thumbnail_folder_base, exist_ok=True)

    # List folders to process from input base directory
    folders = [d for d in os.listdir(input_base_path) if os.path.isdir(os.path.join(input_base_path, d))]
    if not folders:
        logging.info(f"No folders found in {input_base_path} to process.")
        return

    # Calculate the total number of files to be processed across all folders
    total_files = sum([len([f for f in os.listdir(os.path.join(input_base_path, d)) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]) for d in folders])
    logging.info(f"Total files to process across all folders: {total_files}")

    processed_files_ref = [0]  # Use a mutable list to track the total number of processed files

    # Update the database to mark the beginning of processing
    db_operations.update_status(client_code, total_files, processed_files_ref[0])

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(input_base_path, folder)
        logging.info(f"Processing folder: {folder}")
        process_images_in_directory(folder_path, final_folder_base, thumbnail_folder_base, total_files, processed_files_ref, db_operations, client_code)
        
    # Once all files are processed, call the completion function
    processing_completed(client_code, processed_files_ref[0], db_operations)

# Measure time taken for processing
if __name__ == "__main__":
    start_time = time.time()

    # Set database name and collection name
    db_name = "test"  # Replace with your database name
    collection_name = "services"  # Replace with your collection name

    # Create MongoDB operations instance
    db_operations = MongoDBOperations(db_name, collection_name)

    # Specify the client code
    client_code = 'HWW80'  # Replace with your actual client code

    # Fetch data to ensure the processing should start
    data_to_process = db_operations.fetch_data(client_code)

    if data_to_process:
        # Process all folders within the input directory
        process_multiple_folders(f"input/{client_code}", f"output/{client_code}", client_code, db_operations)
    else:
        logging.info(f"No queued data found for clientId {client_code}.")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Time Taken: {elapsed_time:.2f} seconds")
