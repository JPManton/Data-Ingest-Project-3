import requests
from requests import get
import pandas as pd
import json
from functions import write_to_blob, exec_stored_proc
from datetime import date
import os

# ---------------------------------------------------------------------------------------------------------------------
# Next few days weather forecast
# ---------------------------------------------------------------------------------------------------------------------

api_key = "REDACTED"
api_secret = "REDACTED"
latlongs = [
    [51.509865, -0.118092],  # London
    [50.909698, -1.404351],  # Southampton
    [55.953251, -3.188267],  # Edinburgh
    [52.489471, -1.898575],  # Birmingham
    [51.454514, -2.587910],  # Bristol
    [53.801277, -1.548567],  # Leeds
    [53.483959, -2.244644],  # Manchester
    [53.400002, -2.983333],  # Liverpool
    [52.950001, -1.150000],   # Nottingham
    [51.481583, -3.179090]   # Cardiff
]

headers = {
    'X-IBM-Client-Id': api_key,
    'X-IBM-Client-Secret': api_secret,
    'accept': "application/json"
}

print("Phase 1 Complete - Create the lat and longs required")

listoflists = []

for item in latlongs:

    lat = item[0]
    long = item[1]
    url = "https://REDACTED.eu-gb.apiconnect.appdomain.cloud/metoffice/production/v0/forecasts/point/daily?excludeParameterMetadata=false&includeLocationName=true&latitude=" + str(
        lat) + "&longitude=" + str(long)

    r = requests.get(url, headers=headers)
    basedata = pd.DataFrame(r.json())

    for item in basedata["features"]:
        colCity = item["properties"]["location"]["name"]
        # colDate = item["properties"]["modelRunDate"]
        for timeItem in item["properties"]["timeSeries"]:
            colForecastDay = timeItem["time"]
            if "midday10MWindSpeed" in timeItem:
                colmidday10MWindSpeed = timeItem["midday10MWindSpeed"]
            else:
                colmidday10MWindSpeed = 0
            if "middayVisibility" in timeItem:
                colmiddayVisibility = timeItem["middayVisibility"]
            else:
                colmiddayVisibility = 0
            if "middayRelativeHumidity" in timeItem:
                colmiddayRelativeHumidity = timeItem["middayRelativeHumidity"]
            else:
                colmiddayRelativeHumidity = 0
            if "dayUpperBoundMaxFeelsLikeTemp" in timeItem:
                coldayMaxFeelsLikeTemp = timeItem["dayUpperBoundMaxFeelsLikeTemp"]
            else:
                coldayMaxFeelsLikeTemp = 0
            if "dayLowerBoundMaxFeelsLikeTemp" in timeItem:
                coldayMinFeelsLikeTemp = timeItem["dayLowerBoundMaxFeelsLikeTemp"]
            else:
                coldayMinFeelsLikeTemp = 0
            if "dayProbabilityOfPrecipitation" in timeItem:
                coldayProbabilityOfPrecipitation = timeItem["dayProbabilityOfPrecipitation"]
            else:
                coldayProbabilityOfPrecipitation = 0
            if "dayProbabilityOfSnow" in timeItem:
                coldayProbabilityOfSnow = timeItem["dayProbabilityOfSnow"]
            else:
                coldayProbabilityOfSnow = 0
            if "dayProbabilityOfPrecipitation" in timeItem:
                coldayProbabilityOfHeavySnow = timeItem["dayProbabilityOfHeavySnow"]
            else:
                coldayProbabilityOfHeavySnow = 0
            if "dayProbabilityOfRain" in timeItem:
                coldayProbabilityOfRain = timeItem["dayProbabilityOfRain"]
            else:
                coldayProbabilityOfRain = 0
            if "dayProbabilityOfHeavyRain" in timeItem:
                coldayProbabilityOfHeavyRain = timeItem["dayProbabilityOfHeavyRain"]
            else:
                coldayProbabilityOfHeavyRain = 0
            new_col = [
                colCity, colForecastDay, colmidday10MWindSpeed, colmiddayVisibility, colmiddayRelativeHumidity,
                coldayMaxFeelsLikeTemp, coldayMinFeelsLikeTemp, coldayProbabilityOfPrecipitation, coldayProbabilityOfSnow,
                coldayProbabilityOfHeavySnow, coldayProbabilityOfRain, coldayProbabilityOfHeavyRain
            ]
            listoflists.append(new_col)

print("Phase 2 Complete - Loop through and parse out the JSON for all lat longs")

# Create column names for final table
df_columns = [
            "cityName", "ForecastDay", "midday10MWindSpeed", "middayVisibility", "middayRelativeHumidity",
            "dayMaxFeelsLikeTemp", "dayMinFeelsLikeTemp", "dayProbabilityOfPrecipitation", "dayProbabilityOfSnow",
            "dayProbabilityOfHeavySnow", "dayProbabilityOfRain", "dayProbabilityOfHeavyRain"
        ]

# Create a final table from the list of lists and the columns above
forecastData = pd.DataFrame(listoflists, columns=df_columns)

print("Phase 3 Complete - Combine into one base table")

# Export to CSV ready to import to blob
forecastData.to_csv("./forecastDataforSQL.csv", sep="|", index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./forecastDataforSQL.csv", blob_folder="MetOffice",
              container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./forecastDataforSQL.csv")

print("Phase 4 Complete - Write to blob storage")

# Run the stored procedure to bring in the data
exec_stored_proc("exec [playpen].[metoffice_external_to_internal_import]")

print("Phase 5 Complete - Run the stored procedure to bring new data into playpen.jm_met_office_forecast")
