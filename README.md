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

# TODO

I think I'm importing all codes in a refset and looking at the active flag, rather than just looking at the most recent refset map for each "id" and then taking it if it's active. Leads to e.g. refsets 999000061000001102 having 320490009 in the active AND inactive codes, when it should just be inactive.
