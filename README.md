# SENIOR DATA ENGINEER TEST - RUKITA

This test consist of 3 cases : 
- case 1 - daily occupancy
- case 2 - conversion leads
- case 3 - scraping data - mamikost (Optional)

### Note : Image prove it all ingest is successfully - last running 11 Jun 2023

## How to run this application
1. Please setup `virtual-environments`
    ```
        python3 -m venv venv
    ```
2. To activation the `virtual-enviroments`
    ```
        source env/bin/activate
    ```
3. After `virtual-enviroments` has been activate, than install the library
    ```
        pip install -r requirements.txt
    ```
4. rename `.env.example` to `.env` and change to your environment
    ```
        PROJECT_ID = "YOUR_PROJECT_ID"
        DATASET_ID = "YOUR_DATASET_ID"
        LOCATION = "LOCATION_DATASET"
        CREDENTIAL_FILE = "YOUR_PATH_KEY"
    ```
5. Create service Account and give the access `Bigquery Admin` and `Bigquery Job Users` 
6. To run case, please run each file 
    before
    ```
        python3 etl_case1.py
    ```
    and
    ```
        python3 etl_case2.py
    ```
## Case Number 3 - Scrapping
7. To run case number 3, please check Google Chrome Binary and Chrome Driver already installed
    can run example this
    ```
        python3 case3.py --url="https://mamikos.com/kost-promo-ngebut/semua%20kota?from=home%20discount" --csv="dataset/mamikos-listing.csv"
    ```