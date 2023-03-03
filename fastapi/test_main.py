from fastapi.testclient import TestClient
from main import app
import boto3
import os

client = TestClient(app)

# Test login_for_access_token
# def test_login_for_access_token():
#     response = client.post(
#         url = "/token",
#         data = {"username": "damg7245", "password": "spring2023"},
#         auth=("client_id", "client_secret")
#     )
#     assert response.status_code == 200

#Tests the nexrad year list returned
def test_list_years_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.get("/list-years-nexrad")
    assert response.status_code == 200
    message = response.json()["year_list"]
    assert message == ['2023', '2022']

#Tests the nexrad month list of a particular year
def test_list_months_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-months-nexrad?year=2023"
    )
    assert response.status_code == 200

#Tests the nexrad days returned for particular year and month
def test_list_days_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-days-nexrad?year=2023&month=01"
        )
    assert response.status_code == 200

#Tests for the nexrad stations returned for a particular year month day
def test_list_stations_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-stations-nexrad?year=2023&month=01&day=01"
    )
    assert response.status_code == 200
    message = response.json()["stations_list"]
    assert message == [
        "FOP1",
        "KABR",
        "KABX",
        "KAKQ",
        "KAMA",
        "KAMX",
        "KAPX",
        "KARX",
        "KATX",
        "KBBX",
        "KBGM",
        "KBHX",
        "KBIS",
        "KBLX",
        "KBMX",
        "KBOX",
        "KBRO",
        "KBUF",
        "KBYX",
        "KCAE",
        "KCBW",
        "KCBX",
        "KCCX",
        "KCLE",
        "KCLX",
        "KCRP",
        "KCXX",
        "KCYS",
        "KDAX",
        "KDDC",
        "KDFX",
        "KDGX",
        "KDIX",
        "KDLH",
        "KDMX",
        "KDOX",
        "KDTX",
        "KDVN",
        "KDYX",
        "KEAX",
        "KEMX",
        "KENX",
        "KEOX",
        "KEPZ",
        "KESX",
        "KEVX",
        "KEWX",
        "KEYX",
        "KFCX",
        "KFDR",
        "KFDX",
        "KFFC",
        "KFSD",
        "KFSX",
        "KFTG",
        "KFWS",
        "KGGW",
        "KGJX",
        "KGLD",
        "KGRB",
        "KGRK",
        "KGRR",
        "KGWX",
        "KGYX",
        "KHDX",
        "KHGX",
        "KHNX",
        "KHTX",
        "KICT",
        "KICX",
        "KILN",
        "KILX",
        "KIND",
        "KINX",
        "KIWA",
        "KIWX",
        "KJAX",
        "KJGX",
        "KJKL",
        "KLBB",
        "KLCH",
        "KLGX",
        "KLIX",
        "KLNX",
        "KLOT",
        "KLRX",
        "KLSX",
        "KLTX",
        "KLVX",
        "KLWX",
        "KLZK",
        "KMAF",
        "KMAX",
        "KMBX",
        "KMHX",
        "KMKX",
        "KMLB",
        "KMOB",
        "KMPX",
        "KMQT",
        "KMRX",
        "KMSX",
        "KMTX",
        "KMUX",
        "KMVX",
        "KMXX",
        "KNKX",
        "KNQA",
        "KOAX",
        "KOHX",
        "KOKX",
        "KOTX",
        "KPAH",
        "KPBZ",
        "KPDT",
        "KPOE",
        "KPUX",
        "KRAX",
        "KRGX",
        "KRIW",
        "KRLX",
        "KRTX",
        "KSFX",
        "KSGF",
        "KSHV",
        "KSJT",
        "KSOX",
        "KSRX",
        "KTBW",
        "KTFX",
        "KTLH",
        "KTLX",
        "KTWX",
        "KTYX",
        "KUDX",
        "KUEX",
        "KVAX",
        "KVBX",
        "KVNX",
        "KVTX",
        "KVWX",
        "KYUX",
        "PACG",
        "PAEC",
        "PAHG",
        "PAKC",
        "PAPD",
        "PGUA",
        "PHKI",
        "PHKM",
        "PHMO",
        "PHWA",
        "RKJK",
        "RODN",
        "TADW",
        "TATL",
        "TBNA",
        "TBOS",
        "TBWI",
        "TCLT",
        "TCMH",
        "TCVG",
        "TDAL",
        "TDAY",
        "TDCA",
        "TDEN",
        "TDTW",
        "TEWR",
        "TFLL",
        "THOU",
        "TIAD",
        "TIAH",
        "TICH",
        "TIDS",
        "TJFK",
        "TJUA",
        "TLAS",
        "TLVE",
        "TMCI",
        "TMCO",
        "TMDW",
        "TMEM",
        "TMIA",
        "TMKE",
        "TMSP",
        "TMSY",
        "TOKC",
        "TORD",
        "TPHL",
        "TPHX",
        "TPIT",
        "TRDU",
        "TSDF",
        "TSJU",
        "TSLC",
        "TSTL",
        "TTPA",
        "TTUL"
    ]

#test for the nexrad files returned for a particular year month day and station
def test_list_files_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-files-nexrad?year=2023&month=01&day=01&station=KABR"
    )
    assert response.status_code == 200

#Test the url return function
def test_fetch_url_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/fetch-url-nexrad?name=KABR20230101_000142_V06"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://damg-test.s3.amazonaws.com/logs/nexrad/KABR20230101_000142_V06"

#Test for proper function of url validation
def test_validate_url_nexrad():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/validate-url-nexrad?name=%22https%3A%2F%2Fdamg-test.s3.amazonaws.com%2Flogs%2Fnexrad%2FKABR20230101_000142_V06%22"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://noaa-nexrad-level2.s3.amazonaws.com/ps%3A///d/am/%22htt/%22https%3A//damg-test.s3.amazonaws.com/logs/nexrad/KABR20230101_000142_V06%22"


#Tests the goes year list returned
def test_list_years_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.get("/list-years-goes")
    assert response.status_code == 200
    message = response.json()["year_list"]
    assert message == ["2022","2023"]

#Tests the goes days list of a particular year
def test_list_days_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-days-goes?year=2023"
    )
    assert response.status_code == 200
    
#Tests the goes hours list of a particular year and day
def test_list_hours_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-hours-goes?year=2023&day=001"
    )
    assert response.status_code == 200

#Tests the goes files list of a particular year day and hour
def test_files_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/list-files-goes?year=2023&day=001&hour=00"
    )
    assert response.status_code == 200

#Tests the goes files list of a particular year day and hour
def test_fetch_url_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/fetch-url-goes?name=OR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://damg-test.s3.amazonaws.com/logs/goes18/OR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc"

#Tests the validate url goes function
def test_validate_url_goes():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/validate-url-goes?name=%22https%3A%2F%2Fdamg-test.s3.amazonaws.com%2Flogs%2Fgoes18%2FOR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc%22"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/%3A//d/amg/-t/%22https%3A//damg-test.s3.amazonaws.com/logs/goes18/OR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc%22"

#Tests fetch_url_goes_from_name
def test_fetch_url_goes_from_name():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/fetch-url-goes-from-name?name=OR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://damg-test.s3.amazonaws.com/logs/goes18/OR_ABI-L1b-RadC-M6C01_G18_s20230010001170_e20230010003544_c20230010003582.nc"


#Tests fetch_url_nexrad_from_name
def test_fetch_url_nexrad_from_name():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.post(
        url = "/fetch-url-nexrad-from-name?name=KABR20230101_000142_V06"
    )
    assert response.status_code == 200
    message = response.json()["url"]
    assert message == "https://damg-test.s3.amazonaws.com/logs/nexrad/KABR20230101_000142_V06"

#Tests mapping_stations
def test_mapping_stations():
    headers={'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYW1nNzI0NSIsImV4cCI6MTY3NzgxNDM3MH0.x5EEEApt-SHrmT9bQyd__HD-tJUYFrTu2XAlDoSmmN8'}
    response = client.get(
        url = '/mapping-stations'
    )
    assert response.status_code == 200