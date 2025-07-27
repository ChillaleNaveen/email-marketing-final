from google_auth_oauthlib.flow import InstalledAppFlow

# 👇 SCOPES includes both read and send access
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send'
]

def main():
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json', SCOPES)

    creds = flow.run_local_server(port=0)

    with open('token.json', 'w') as token:
        token.write(creds.to_json())

if __name__ == '__main__':
    main()
