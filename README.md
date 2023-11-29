# Get latest refset

Execute

```
node index.js
```

This gets the latest drug ref sets from TRUD. Need the following env variables in `.env`:

- email (email used for TRUD subscription login)
- password (password used for TRUD subscription login)
- ACCESS_KEY_ID (this, and the 3 below are all related to the Cloudflare S3 api for uploading files to R2)
- SECRET_ACCESS_KEY
- ACCOUNT_ID

# View refsets

Execute

```
npm start
```

Opens a web page. Should be self-explanatory.

# Deploy

Every push to `main` branch results in a cloudflare deployment at https://nhs-drug-refset.pages.dev/.
