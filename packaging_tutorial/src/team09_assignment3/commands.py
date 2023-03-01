import click
import requests
import typer
import getpass

app = typer.Typer()

user_bucket_name = 'damg-test'
subscription_tiers = ['free', 'gold', 'platinum']
BASE_URL = 'http://34.74.233.133:8090'


@app.command("create_user")
def create_user():

    # Prompt the user for username
    username = typer.prompt("Enter email")

    # Make a request to the endpoint to check if the username already exists
    username_response = (requests.post(BASE_URL + f'/check-user-exists?username={username}')).json()
    status = username_response["user"]

    if status:
        typer.echo("Username already exists.")
        raise typer.Abort()

    # Prompt the user for password
    password = getpass.getpass(prompt='Enter password: ')
    
    # Prompt the user to re-enter password for verification
    password2 = getpass.getpass(prompt='Re-enter password: ')
    
    # Verify that the passwords match
    if password != password2:
        typer.echo("Passwords don't match.")
        raise typer.Abort()
    
    # Prompt the user for full name
    full_name = typer.prompt('Enter full name: ')
    
    # Prompt the user to select a subscription tier
    tier = typer.prompt("Select subscription tier", type=click.Choice(subscription_tiers))

    requests.post(BASE_URL + f'/add-user?username={username}&password={password}&full_name={full_name}&tier={tier}')
    
    # Code to create user goes here
    typer.echo(f"User {username} created successfully with name {full_name} and subscription tier {tier}.")


@app.command("download")
def download(filename: str):

    """
    Downloads a file with the specified filename and returns the URL of the file moved to your S3 location.
    """

    file_url_response = requests.post(BASE_URL + f'/download?filename={filename}').json()
    file_url = file_url_response["url"]       

    typer.echo(file_url) 


@app.command("fetch_goes")
def fetch_goes(bucket: str, file_prefix: str, year: str, day: str, hour: str):
    """
    Lists all files in the specified bucket that match the file prefix and time parameters.
    """

    # Make a request to the endpoint to retrieve the list of files for the selected file prefix, year, day and hour
    file_list_response = (requests.post(BASE_URL + f'/fetch-goes?file_prefix={file_prefix}&year={year}&day={day}&hour={hour}')).json()
    file_list = file_list_response["file_list"]

    typer.echo(f"Files in bucket {bucket} matching prefix {file_prefix} and time {year}-{day}T{hour}:00:00:")

    for file in file_list:
        typer.echo(file)


@app.command("fetch_nexrad")
def fetch_nexrad(year: str, month: str, day: str, station: str):
    """
    Lists all files in the specified bucket that match the file prefix and time parameters.
    """

    # Make a request to the endpoint to retrieve the list of files for the selected year, day and hour
    file_list_response = (requests.post(BASE_URL + f'/fetch-nexrad?year={year}&month={month}day={day}&station={station}')).json()
    file_list = file_list_response["file_list"]

    typer.echo(f"Files in noaa-nexrad-level2 bucket matching the station {station} and date {day}-{month}-{year}")

    for file in file_list:
        typer.echo(file)


if __name__ == "__main__":
    app() 