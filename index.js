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
  write,
  writeFileSync,
} from 'fs';
import { Readable } from 'stream';
import { finished } from 'stream/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import 'dotenv/config';
import decompress from 'decompress';

const __dirname = dirname(fileURLToPath(import.meta.url));

let Cookie;

const FILES_DIR = join(__dirname, 'files');
const ZIP_DIR = join(FILES_DIR, 'zip');
const RAW_DIR = join(FILES_DIR, 'raw');
const PROCESSED_DIR = join(FILES_DIR, 'processed');

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
  const outputFile = join(ZIP_DIR, filename);
  const stream = fs.createWriteStream(outputFile);
  const { body } = await fetch(url, { headers: { Cookie } });
  await finished(Readable.fromWeb(body).pipe(stream));
  console.log(`> File downloaded.`);
  return filename;
}

async function extractZip(zipFile) {
  const name = zipFile.replace('.zip', '');
  const file = join(ZIP_DIR, zipFile);
  const outDir = join(RAW_DIR, name);
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

function loadDataIntoMemory(dir) {
  const rawFilesDir = join(RAW_DIR, dir);
  const processedFilesDir = join(PROCESSED_DIR, dir);
  const definitionFile = join(processedFilesDir, 'defs.json');
  const refSetFile = join(processedFilesDir, 'refSets.json');
  if (existsSync(definitionFile) && existsSync(refSetFile)) {
    console.log(`> The json files already exist so I'll just load them...`);
    const definitions = JSON.parse(readFileSync(definitionFile));
    const refSets = JSON.parse(readFileSync(definitionFile));
    return { definitions, refSets };
  }
  if (!existsSync(processedFilesDir)) {
    mkdirSync(processedFilesDir);
  }
  const DRUG_DIR = join(
    rawFilesDir,
    readdirSync(rawFilesDir).filter((x) => x.indexOf('Drug') > -1)[0]
  );
  const REFSET_DIR = join(DRUG_DIR, 'Full', 'Refset', 'Content');
  const refsetFile = join(
    REFSET_DIR,
    readdirSync(REFSET_DIR).filter((x) => x.indexOf('Simple') > -1)[0]
  );
  const refSets = {};
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
      if (!refSets[refsetId])
        refSets[refsetId] = { activeConcepts: [], inactiveConcepts: [] };
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

  const TERM_DIR = join(DRUG_DIR, 'Full', 'Terminology');
  const descFile = join(
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
    defs.sort((a, b) => {
      if (a.active && !b.active) return -1;
      if (b.active && !a.active) return 1;
      if (a.effectiveTime > b.effectiveTime) return -1;
      if (a.effectiveTime < b.effectiveTime) return 1;
      return 0;
    }); //todo up to here
    simpleDefs[conceptId] = defs[0];
  });

  const simpleRefSets = {};

  Object.keys(refSets).forEach((refSetId) => {
    if (!simpleDefs[refSetId])
      console.log(`No description for refset with id: ${refSetId}`);
    else {
      const def = simpleDefs[refSetId].term;
      if (simpleRefSets[def])
        console.log(`There is already an entry for: ${def}`);
      else {
        simpleRefSets[def] = refSets[refSetId];
      }
    }
  });

  writeFileSync(definitionFile, JSON.stringify(simpleDefs, null, 2));
  writeFileSync(refSetFile, JSON.stringify(simpleRefSets, null, 2));

  return { definitions: simpleDefs, refSets };
}

function rest() {
  const versions = readdirSync(PROCESSED_DIR).filter((x) => x !== '.gitignore');
  writeFileSync(
    join(__dirname, 'routes.json'),
    JSON.stringify(versions, null, 2)
  );
}

// Get latest TRUD version
getLatestUrl()
  .then(downloadIfNotExists)
  .then(extractZip)
  .then(loadDataIntoMemory)
  .then(rest);
