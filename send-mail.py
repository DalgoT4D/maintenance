import os
import sendgrid
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv


load_dotenv(".env")
# Set the sender and recipient email addresses
SENDER = os.getenv("SENDER")
RECIPIENT = os.getenv("RECIPIENT")

# Set the subject and body
SUBJECT = "Alert: .env file not found"
BODY_TEXT = "The .env file is missing."

# Create a new SendGrid client
# Retrieve the API key from the environment
API_KEY = os.getenv("API_KEY")

# Create a new SendGrid client
sg = sendgrid.SendGridAPIClient(api_key=API_KEY)

# Create a new Mail object
message = Mail(
    from_email=SENDER,
    to_emails=RECIPIENT,
    subject=SUBJECT,
    plain_text_content=BODY_TEXT)

try:
    # Send the email
    response = sg.send(message)
    print(f"Email sent! Status code: {response.status_code}")
except Exception as e:
    print(f"An error occurred: {str(e)}")
