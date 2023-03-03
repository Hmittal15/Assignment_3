#BasiCLI

A small package to fetch files from GOES18 and nexrad public S3 buckets

@app.command("create_user"):
It defines a function called 'create_user' that prompts the user to enter their username, password, email, full name, and subscription tier. The function then makes a request to an API endpoint to check if the username already exists. If the username does not exist, the function prompts the user to enter their password and verify it. Then, it makes a request to the API endpoint to create a new user with the provided information.

@app.command("download"):
This function downloads a file from the S3 bucket.

It prompts the user to enter their username and password and checks if the user has exceeded their API call limit. If the user has not exceeded their API call limit, it makes a request to the download endpoint with the provided filename and headers, and receives a response containing the URL of the file. If the user has exceeded their API call limit, it outputs an error message to the user.

@app.command("fetch_goes"):
This function fetches the list of files present in GOES18 S3 bucket according to the path provided by the user .

It prompts the user to enter their username and password and checks if the user has exceeded their API call limit. If the user has not exceeded their API call limit, it makes a request to the listing endpoint with the provided filename and headers, and receives a response containing the list of files. If the user has exceeded their API call limit, it outputs an error message to the user.

@app.command("fetch_nexrad"):
This function fetches the list of files present in Nexrad S3 bucket according to the path provided by the user .

It prompts the user to enter their username and password and checks if the user has exceeded their API call limit. If the user has not exceeded their API call limit, it makes a request to the listing endpoint with the provided filename and headers, and receives a response containing the list of files. If the user has exceeded their API call limit, it outputs an error message to the user.