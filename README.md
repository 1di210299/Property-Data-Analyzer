Property Data Analyzer
This project is a comprehensive Python script to scrape property data from homes.com, geocode property addresses, match them with broadband availability data, and identify nearby amenities such as Starbucks locations and schools. The final result is an analysis file that can be sent to an email.

Overview
The main objectives of this project are:

Scrape property listings for selected cities in North Carolina.
Download broadband availability data from the FCC API.
Geocode the properties' addresses.
Match each property with available internet providers and their speeds.
Identify nearby amenities (Starbucks and schools) using Overpass API.
Send the final results via email as an attached CSV file.
Table of Contents
Prerequisites
Setup and Installation
Configuration
Usage
Script Details
Notes
License
Prerequisites
Before using the project, ensure you have the following installed:

Python 3.x
pip (Python package manager)
Chrome browser (for Selenium-based scraping)
A .env file with proper credentials for the FCC API.
Setup and Installation
Clone the Repository

bash
Copiar código
git clone https://github.com/your_username/property-data-analyzer.git
cd property-data-analyzer
Install Dependencies Ensure you have all the required Python packages installed by running:

bash
Copiar código
pip install -r requirements.txt
Environment Variables Create a .env file in the root directory with your credentials and API keys. Example:

makefile
Copiar código
username=<your_fcc_api_username>
hash_value=<your_fcc_api_hash_value>
Configuration
Chrome WebDriver
Ensure chromedriver is installed and available in your PATH or let webdriver_manager handle the installation automatically.

FCC API
The script uses the FCC API to fetch broadband availability data. You need a username and hash value to access the data, which should be added to the .env file.

Google Maps API
The script optionally uses Google Maps API for geocoding. Add your Google API key to the .env file:

makefile
Copiar código
google_api_key=<your_google_maps_api_key>
Usage
To run the main script:

bash
Copiar código
python main.py
This will execute all the processes in sequence, from downloading broadband data to sending the final CSV via email.

Modules Breakdown
Download Broadband Data (download_broadband_data): Fetches the most recent broadband data for North Carolina from the FCC API.

Extract Property Data (extract_property_data): Scrapes property listings from homes.com based on specific search criteria and saves the data to a CSV file.

Geocode Addresses (geocode_addresses): Geocodes the property addresses using either Nominatim (OpenStreetMap) or Google Maps API, adding latitude and longitude to each property.

Find Internet Providers (find_internet_providers): Matches properties with available internet providers using H3 geospatial indexing and GeoPandas for spatial analysis.

Find Nearby Amenities (find_nearby_amenities): Uses Overpass API to locate amenities like Starbucks and schools within a specified radius of each property.

Send Email (send_email): Sends the final CSV with all the processed data to a specified email address.

Notes
Geocoding Services: The script uses both Nominatim (OpenStreetMap) and Google Maps API for geocoding. Ensure you adhere to the respective API usage policies and rate limits.
Overpass API: Overpass API is used to fetch nearby amenities. If you encounter rate limits, the script is designed to handle them and retry after a brief wait.
Selenium Scraping: Since the script relies on web scraping, the scraping logic might need to be updated if homes.com changes its layout or structure.
Sending Emails: Make sure you use an email service that allows sending emails programmatically through SMTP.
License
This project is licensed under the MIT License - see the LICENSE file for details.


