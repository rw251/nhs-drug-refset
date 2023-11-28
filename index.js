/*
 * 1. Gets the latest zip file from the TRUD website (unless already downloaded)
 *  - You need a TRUD account
 *  - Put your login detatils in the .env file
 *  - Make sure you are subscribed to "SNOMED CT UK Drug Extension, RF2: Full, Snapshot & Delta"
 * 2.
 *
 */

import fs, {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'fs';
import { Readable } from 'stream';
import { finished } from 'stream/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import 'dotenv/config';
import decompress from 'decompress';
import { compress } from 'brotli';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let Cookie;

const FILES_DIR = path.join(__dirname, 'files');
const ZIP_DIR = path.join(FILES_DIR, 'zip');
const RAW_DIR = path.join(FILES_DIR, 'raw');
const PROCESSED_DIR = path.join(FILES_DIR, 'processed');

const existingFiles = fs.readdirSync(ZIP_DIR);

if (!process.env.email) {
  console.log('Need email=xxx in the .env file');
  process.exit();
}
if (!process.env.password) {
  console.log('Need password=xxx in the .env file');
  process.exit();
}

async function login() {
  if (Cookie) return;
  const email = process.env.email;
  const password = process.env.password;

  console.log('> Logging in to TRUD...');
  const result = await fetch(
    'https://isd.digital.nhs.uk/trud/security/j_spring_security_check',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      redirect: 'manual',
      body: new URLSearchParams({
        j_username: email,
        j_password: password,
        commit: 'LOG+IN',
      }),
    }
  );
  const cookies = result.headers.getSetCookie();
  const cookie = cookies.filter((x) => x.indexOf('JSESSIONID') > -1)[0];
  console.log('> Logged in, and cookie cached.');
  Cookie = cookie;
}

async function getLatestUrl() {
  await login();
  const response = await fetch(
    'https://isd.digital.nhs.uk/trud/users/authenticated/filters/0/categories/26/items/105/releases?source=summary',
    { headers: { Cookie } }
  );
  const html = await response.text();
  const downloads = html.match(
    /href="(https:\/\/isd.digital.nhs.uk\/download[^"]+)"/
  );
  const latest = downloads[1];
  return latest;
}

async function downloadIfNotExists(url) {
  await login();

  const filename = url.split('/').reverse()[0].split('?')[0];
  console.log(`> The most recent zip file on TRUD is ${filename}`);

  if (existingFiles.indexOf(filename) > -1) {
    console.log(`> The zip file already exists so no need to download again.`);
    return filename;
  }

  console.log(`> That zip is not stored locally. Downloading...`);
  const outputFile = path.join(ZIP_DIR, filename);
  const stream = fs.createWriteStream(outputFile);
  const { body } = await fetch(url, { headers: { Cookie } });
  await finished(Readable.fromWeb(body).pipe(stream));
  console.log(`> File downloaded.`);
  return filename;
}

async function extractZip(zipFile) {
  const name = zipFile.replace('.zip', '');
  const file = path.join(ZIP_DIR, zipFile);
  const outDir = path.join(RAW_DIR, name);
  if (existsSync(outDir)) {
    console.log(
      `> The directory ${outDir} already exists, so I'm not unzipping.`
    );
    return name;
  }
  console.log(`> The directory ${outDir} does not yet exist. Creating...`);
  mkdirSync(outDir);
  console.log(`> Extracting files from the zip...`);
  const files = await decompress(file, outDir, {
    filter: (file) => {
      if (file.path.toLowerCase().indexOf('full') > -1) return true;
      if (file.path.toLowerCase().indexOf('readme') > -1) return true;
      if (file.path.toLowerCase().indexOf('information') > -1) return true;
      return false;
    },
  });
  console.log(`> ${files.length} files extracted.`);
  return name;
}

function getFileNames(dir, startingFromProjectDir) {
  const rawFilesDir = path.join(RAW_DIR, dir);
  const processedFilesDirFromRoot = path.join(PROCESSED_DIR, dir);
  const processedFilesDir = startingFromProjectDir
    ? path.join('files', 'processed', dir)
    : processedFilesDirFromRoot;
  const definitionFile = path.join(processedFilesDir, 'defs.json');
  const refSetFile1 = path.join(processedFilesDir, 'refSets-0-9999.json');
  const refSetFile2 = path.join(processedFilesDir, 'refSets-10000+.json');
  const definitionFileBrotli = path.join(
    processedFilesDirFromRoot,
    'defs.json.br'
  );
  const refSetFile1Brotli = path.join(
    processedFilesDirFromRoot,
    'refSets-0-9999.json.br'
  );
  const refSetFile2Brotli = path.join(
    processedFilesDirFromRoot,
    'refSets-10000+.json.br'
  );
  return {
    rawFilesDir,
    definitionFile,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli,
    refSetFile1Brotli,
    refSetFile2Brotli,
  };
}

function loadDataIntoMemory(dir) {
  const { rawFilesDir, definitionFile, refSetFile1, refSetFile2 } =
    getFileNames(dir);
  if (
    existsSync(definitionFile) &&
    existsSync(refSetFile1) &&
    existsSync(refSetFile2)
  ) {
    console.log(`> The json files already exist so I'll move on...`);
    // const definitions = JSON.parse(readFileSync(definitionFile));
    // const refSets = JSON.parse(readFileSync(definitionFile));
    return dir;
  }
  if (!existsSync(processedFilesDir)) {
    mkdirSync(processedFilesDir);
  }
  const DRUG_DIR = path.join(
    rawFilesDir,
    readdirSync(rawFilesDir).filter((x) => x.indexOf('Drug') > -1)[0]
  );
  const REFSET_DIR = path.join(DRUG_DIR, 'Full', 'Refset', 'Content');
  const refsetFile = path.join(
    REFSET_DIR,
    readdirSync(REFSET_DIR).filter((x) => x.indexOf('Simple') > -1)[0]
  );
  const refSets = {};
  const allConcepts = {};
  readFileSync(refsetFile, 'utf8')
    .split('\n')
    .forEach((row) => {
      const [
        id,
        effectiveTime,
        active,
        moduleId,
        refsetId,
        referencedComponentId,
      ] = row.replace(/\r/g, '').split('\t');
      if (!refSets[refsetId]) {
        allConcepts[refsetId] = true;
        refSets[refsetId] = { activeConcepts: [], inactiveConcepts: [] };
      }
      allConcepts[referencedComponentId] = true;
      if (active === '1') {
        refSets[refsetId].activeConcepts.push(referencedComponentId);
      } else {
        refSets[refsetId].inactiveConcepts.push(referencedComponentId);
      }
    });
  console.log(
    `> Ref set file loaded. It has ${Object.keys(refSets).length} rows.`
  );

  const definitions = {};

  const TERM_DIR = path.join(DRUG_DIR, 'Full', 'Terminology');
  const descFile = path.join(
    TERM_DIR,
    readdirSync(TERM_DIR).filter((x) => x.indexOf('_Description_') > -1)[0]
  );
  readFileSync(descFile, 'utf8')
    .split('\n')
    .forEach((row) => {
      const [
        id,
        effectiveTime,
        active,
        moduleId,
        conceptId,
        languageCode,
        typeId,
        term,
        caseSignificanceId,
      ] = row.replace(/\r/g, '').split('\t');
      if (!definitions[conceptId]) definitions[conceptId] = [];
      if (active === '1') {
        definitions[conceptId].push({
          active: active === '1',
          term,
          effectiveTime,
          isSynonym: typeId === '900000000000013009',
        });
      } else {
        definitions[conceptId].push({
          active: active === '1',
          term,
          effectiveTime,
          isSynonym: typeId === '900000000000013009',
        });
      }
    });
  //
  console.log(
    `> Description file loaded. It has ${Object.keys(definitions).length} rows.`
  );
  const simpleDefs = {};
  Object.entries(definitions).forEach(([conceptId, defs]) => {
    if (!allConcepts[conceptId]) return;
    defs.sort((a, b) => {
      if (a.active && !b.active) return -1;
      if (b.active && !a.active) return 1;
      if (a.effectiveTime > b.effectiveTime) return -1;
      if (a.effectiveTime < b.effectiveTime) return 1;
      return 0;
    }); //todo up to here
    simpleDefs[conceptId] = defs[0];
  });

  const simpleRefSetsLT10000 = {};
  const simpleRefSets10000PLUS = {};

  Object.keys(refSets).forEach((refSetId) => {
    if (!simpleDefs[refSetId])
      console.log(`No description for refset with id: ${refSetId}`);
    else {
      let simpleRefSets =
        refSets[refSetId].activeConcepts.length +
          refSets[refSetId].inactiveConcepts.length <
        10000
          ? simpleRefSetsLT10000
          : simpleRefSets10000PLUS;
      const def = simpleDefs[refSetId].term;
      if (simpleRefSets[def])
        console.log(`There is already an entry for: ${def}`);
      else {
        simpleRefSets[def] = refSets[refSetId];
      }
    }
  });

  writeFileSync(definitionFile, JSON.stringify(simpleDefs, null, 2));
  writeFileSync(refSetFile1, JSON.stringify(simpleRefSetsLT10000, null, 2));
  writeFileSync(refSetFile2, JSON.stringify(simpleRefSets10000PLUS, null, 2));

  return dir;
}

function brot(file, fileBrotli) {
  console.log(`> Compressing ${file}...`);
  const result = compress(readFileSync(file), {
    extension: 'br',
    quality: 11, //compression level - 11 is max
  });
  console.log(`> Compressed. Writing to ${fileBrotli}...`);
  fs.writeFileSync(fileBrotli, result);
}

function compressJson(dir) {
  const {
    definitionFile,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli,
    refSetFile1Brotli,
    refSetFile2Brotli,
  } = getFileNames(dir);
  if (
    existsSync(definitionFileBrotli) &&
    existsSync(refSetFile1Brotli) &&
    existsSync(refSetFile2Brotli)
  ) {
    console.log(`> The brotli files already exist so I'll move on...`);
    // const definitions = JSON.parse(readFileSync(definitionFile));
    // const refSets = JSON.parse(readFileSync(definitionFile));
    return dir;
  }

  console.log('> Starting compression. TAKES A WHILE - GO GET A CUP OF TEA!');

  brot(refSetFile1, refSetFile1Brotli);
  brot(refSetFile2, refSetFile2Brotli);
  brot(definitionFile, definitionFileBrotli);
  console.log(`> All compressed.`);
  return dir;
}

function rest() {
  const versions = readdirSync(PROCESSED_DIR).filter((x) => x !== '.gitignore');
  writeFileSync(
    path.join(__dirname, 'routes.json'),
    JSON.stringify(versions, null, 2)
  );
}

import {
  S3Client,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

let s3;
async function uploadToS3(file, brotliFile) {
  const posixFilePath = file.split(path.sep).join(path.posix.sep);
  const params = {
    Bucket: 'nhs-drug-refset',
    Key: posixFilePath,
  };

  const exists = await s3
    .send(new HeadObjectCommand(params))
    .then((x) => {
      console.log(`> ${file} already exists in R2 so skipping...`);
      return true;
    })
    .catch((err) => {
      if (err.name === 'NotFound') return false;
    });

  if (!exists) {
    console.log(`> ${file} does not exist in R2. Uploading...`);
    await s3.send(
      new PutObjectCommand({
        Bucket: 'nhs-drug-refset',
        Key: posixFilePath,
        Body: readFileSync(brotliFile),
        ContentEncoding: 'br',
        ContentType: 'application/json',
      })
    );
    console.log('> Uploaded.');
  }
}

async function uploadToR2(dir) {
  const accessKeyId = `${process.env.ACCESS_KEY_ID}`;
  const secretAccessKey = `${process.env.SECRET_ACCESS_KEY}`;
  const endpoint = `https://${process.env.ACCOUNT_ID}.r2.cloudflarestorage.com`;

  const {
    definitionFile,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli,
    refSetFile1Brotli,
    refSetFile2Brotli,
  } = getFileNames(dir, true);

  s3 = new S3Client({
    region: 'auto',
    credentials: {
      accessKeyId,
      secretAccessKey,
    },
    endpoint,
  });
  await uploadToS3(definitionFile, definitionFileBrotli);
  await uploadToS3(refSetFile1, refSetFile1Brotli);
  await uploadToS3(refSetFile2, refSetFile2Brotli);
}

// Get latest TRUD version
getLatestUrl()
  .then(downloadIfNotExists)
  .then(extractZip)
  .then(loadDataIntoMemory)
  .then(compressJson)
  .then(uploadToR2)
  .then(rest);
