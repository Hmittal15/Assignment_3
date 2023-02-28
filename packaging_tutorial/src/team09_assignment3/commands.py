import click
import typer
import basic_func
import getpass

app = typer.Typer()

user_bucket_name = 'damg-test'
subscription_tiers = ['free', 'gold', 'platinum']


@app.command("create_user")
def create_user(username: str):

    # Prompt the user for password
    password = getpass.getpass(prompt='Enter password: ')
    
    # Prompt the user to re-enter password for verification
    password2 = getpass.getpass(prompt='Re-enter password: ')
    
    # Verify that the passwords match
    if password != password2:
        typer.echo("Passwords don't match.")
        raise typer.Abort()
    
    # Prompt the user for email
    email = typer.prompt("Enter email")
    
    # Prompt the user to select a subscription tier
    tier = typer.prompt("Select subscription tier", type=click.Choice(subscription_tiers))
    
    # Code to create user goes here
    typer.echo(f"User {username} created successfully with email {email} and subscription tier {tier}.")


@app.command("download")
def download(filename: str):

    """
    Downloads a file with the specified filename and returns the URL of the file moved to your S3 location.
    """

    if (filename == ""): 
        typer.echo("Please enter file name")
        
    else:

        # Checks if it is a file from GOES18 bucket
        if (filename[:16] == 'OR_ABI-L1b-RadC-'):
                
            # Generate file path from filename
            src_object_key = basic_func.path_from_filename_goes(filename)

            # Checks if the provided file exists in goes bucket
            if basic_func.check_if_file_exists_in_s3_bucket('noaa-goes18', src_object_key):

                # Define path where the file has to be written
                user_object_key = f'logs/goes18/{filename}'

                # Copy file from GOES18 bucket to user bucket
                basic_func.copy_to_public_bucket('noaa-goes18', src_object_key, user_bucket_name, user_object_key)

                # Generate link from user bucket
                aws_url = basic_func.generate_download_link_goes(user_bucket_name, user_object_key)

                # Returns the generated URL
                typer.echo (aws_url.split("?")[0])
            
            else:
                # Returns a message saying file does not exist in the bucket
                typer.echo ("404: File not found")

        else:

            # Generate file path from filename
            src_object_key = basic_func.path_from_filename_nexrad(filename) 

            # Checks if the provided file exists in nexrad bucket
            if basic_func.check_if_file_exists_in_s3_bucket('noaa-nexrad-level2', src_object_key):

                # Define path where the file has to be written
                user_object_key = f'logs/nexrad/{filename}'

                # Copy file from nexrad bucket to user bucket
                basic_func.copy_to_public_bucket('noaa-nexrad-level2', src_object_key, user_bucket_name, user_object_key)

                # Generate link from user bucket
                aws_url = basic_func.generate_download_link_nexrad(user_bucket_name, user_object_key)

                # Returns the generated URL
                typer.echo (aws_url.split("?")[0])
            
            else:
                # Returns a message saying file does not exist in the bucket
                typer.echo ("404: File not found")               


@app.command("fetch_goes")
def fetch_goes(bucket: str, file_prefix: str, year: str, day_of_year: str, hour: str):
    """
    Lists all files in the specified bucket that match the file prefix and time parameters.
    """

    # Lists the files present in the goes18 bucket for the selected file_prefix, year, day and hour
    files = basic_func.list_filenames_goes(file_prefix, year, day_of_year, hour)

    typer.echo(f"Files in bucket {bucket} matching prefix {file_prefix} and time {year}-{day_of_year}T{hour}:00:00:")

    for file in files:
        typer.echo(file)


@app.command("fetch_nexrad")
def fetch_nexrad(year: str, month: str, day: str, station: str):
    """
    Lists all files in the specified bucket that match the file prefix and time parameters.
    """

    # Lists the files present in the nexrad bucket for the selected year, month, day and station
    files = basic_func.list_filenames_nexrad(year, month, day, station)

    typer.echo(f"Files in noaa-nexrad-level2 bucket matching the station {station} and date {day}-{month}-{year}")

    for file in files:
        typer.echo(file)


if __name__ == "__main__":
    app() 