/*
 * 1. Gets the latest zip file from the TRUD website (unless already downloaded)
 *  - You need a TRUD account
 *  - Put your login detatils in the .env file
 *  - Make sure you are subscribed to "SNOMED CT UK Drug Extension, RF2: Full, Snapshot & Delta"
 * 2.
 *
 */

import {
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "fs";
import { Readable } from "stream";
import { finished } from "stream/promises";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";
import decompress from "decompress";
import { compress } from "brotli";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const FILES_DIR = path.join(__dirname, "files");
const ZIP_DIR = path.join(FILES_DIR, "zip");
const RAW_DIR = path.join(FILES_DIR, "raw");
const PROCESSED_DIR = path.join(FILES_DIR, "processed");
const CODE_LOOKUP = path.join(FILES_DIR, "code-lookup.json");

const existingFiles = readdirSync(ZIP_DIR);

function ensureDir(filePath, isDir) {
  mkdirSync(isDir ? filePath : path.dirname(filePath), { recursive: true });
  return filePath;
}

// Require API token for TRUD API access
if (!process.env.TRUD_API_KEY) {
  console.log("Need TRUD_API_KEY=xxx in the .env file");
  process.exit();
}

// We use the TRUD API via TRUD_API_KEY.

async function getLatestUrl() {
  const apiToken = process.env.TRUD_API_KEY;
  console.log("> Fetching releases from TRUD API...");
  const url = `https://isd.digital.nhs.uk/trud/api/v1/keys/${apiToken}/items/105/releases`;
  const res = await fetch(url);
  if (!res.ok) {
    console.log(`> Failed to fetch releases: ${res.status} ${res.statusText}`);
    process.exit(1);
  }
  const data = await res.json();
  if (!data.releases || data.releases.length === 0) {
    console.log("> No releases returned from TRUD API");
    process.exit(1);
  }
  // Choose latest by releaseDate
  const latestRelease = data.releases
    .slice()
    .sort((a, b) => new Date(b.releaseDate) - new Date(a.releaseDate))[0];
  const latest = latestRelease.archiveFileUrl;
  console.log(`> Latest archive URL from API: ${latest}`);
  return latest;
}

async function downloadIfNotExists(url) {
  const filename = url.split("/").reverse()[0].split("?")[0];
  console.log(`> The most recent zip file on TRUD is ${filename}`);

  if (existingFiles.indexOf(filename) > -1) {
    console.log(`> The zip file already exists so no need to download again.`);
    return filename;
  }

  console.log(`> That zip is not stored locally. Downloading...`);
  const outputFile = path.join(ZIP_DIR, filename);
  const stream = createWriteStream(ensureDir(outputFile));
  const { body } = await fetch(url);
  await finished(Readable.fromWeb(body).pipe(stream));
  console.log(`> File downloaded.`);
  return filename;
}

async function extractZip(zipFile) {
  const name = zipFile.replace(".zip", "");
  const file = path.join(ZIP_DIR, zipFile);
  const outDir = path.join(RAW_DIR, name);
  if (existsSync(outDir)) {
    console.log(`> The directory ${outDir} already exists, so I'm not unzipping.`);
    return name;
  }
  console.log(`> The directory ${outDir} does not yet exist. Creating...`);
  ensureDir(outDir, true);
  console.log(`> Extracting files from the zip...`);
  const files = await decompress(file, outDir, {
    filter: (file) => {
      if (file.path.toLowerCase().indexOf("full") > -1) return true;
      if (file.path.toLowerCase().indexOf("readme") > -1) return true;
      if (file.path.toLowerCase().indexOf("information") > -1) return true;
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
    ? path.join("files", "processed", dir)
    : processedFilesDirFromRoot;
  const definitionFile1 = path.join(processedFilesDir, "defs-0-9999.json");
  const definitionFile2 = path.join(processedFilesDir, "defs-10000+.json");
  const refSetFile1 = path.join(processedFilesDir, "refSets-0-9999.json");
  const refSetFile2 = path.join(processedFilesDir, "refSets-10000+.json");
  const definitionFileBrotli1 = path.join(processedFilesDirFromRoot, "defs-0-9999.json.br");
  const definitionFileBrotli2 = path.join(processedFilesDirFromRoot, "defs-10000+.json.br");
  const refSetFile1Brotli = path.join(processedFilesDirFromRoot, "refSets-0-9999.json.br");
  const refSetFile2Brotli = path.join(processedFilesDirFromRoot, "refSets-10000+.json.br");
  return {
    rawFilesDir,
    definitionFile1,
    definitionFile2,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli1,
    definitionFileBrotli2,
    refSetFile1Brotli,
    refSetFile2Brotli,
    processedFilesDir,
    processedFilesDirFromRoot,
  };
}

async function loadDataIntoMemory(dir) {
  const {
    processedFilesDirFromRoot,
    rawFilesDir,
    definitionFile1,
    definitionFile2,
    refSetFile1,
    refSetFile2,
  } = getFileNames(dir);
  if (
    existsSync(definitionFile1) &&
    existsSync(definitionFile2) &&
    existsSync(refSetFile1) &&
    existsSync(refSetFile2)
  ) {
    console.log(`> The json files already exist so I'll move on...`);
    // const definitions = JSON.parse(readFileSync(definitionFile));
    // const refSets = JSON.parse(readFileSync(definitionFile));
    return dir;
  }
  if (!existsSync(processedFilesDirFromRoot)) {
    mkdirSync(processedFilesDirFromRoot);
  }
  const DRUG_DIR = path.join(
    rawFilesDir,
    readdirSync(rawFilesDir).filter((x) => x.indexOf("Drug") > -1)[0]
  );
  const REFSET_DIR = path.join(DRUG_DIR, "Full", "Refset", "Content");
  const refsetFile = path.join(
    REFSET_DIR,
    readdirSync(REFSET_DIR).filter((x) => x.indexOf("Simple") > -1)[0]
  );
  const refSets = {};
  const allConcepts = {};
  readFileSync(refsetFile, "utf8")
    .split("\n")
    .forEach((row) => {
      const [id, effectiveTime, active, moduleId, refsetId, referencedComponentId] = row
        .replace(/\r/g, "")
        .split("\t");
      if (!refSets[refsetId]) {
        allConcepts[refsetId] = true;
        refSets[refsetId] = { activeConcepts: [], inactiveConcepts: [] };
      }
      allConcepts[referencedComponentId] = true;
      if (active === "1") {
        refSets[refsetId].activeConcepts.push(referencedComponentId);
      } else {
        refSets[refsetId].inactiveConcepts.push(referencedComponentId);
      }
    });
  console.log(`> Ref set file loaded. It has ${Object.keys(refSets).length} rows.`);

  const definitions = {};

  const TERM_DIR = path.join(DRUG_DIR, "Full", "Terminology");
  const descFile = path.join(
    TERM_DIR,
    readdirSync(TERM_DIR).filter((x) => x.indexOf("_Description_") > -1)[0]
  );
  readFileSync(descFile, "utf8")
    .split("\n")
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
      ] = row.replace(/\r/g, "").split("\t");
      if (!definitions[conceptId]) definitions[conceptId] = [];
      if (active === "1") {
        definitions[conceptId].push({
          active: active === "1",
          term,
          effectiveTime,
          isSynonym: typeId === "900000000000013009",
        });
      } else {
        definitions[conceptId].push({
          active: active === "1",
          term,
          effectiveTime,
          isSynonym: typeId === "900000000000013009",
        });
      }
    });
  //
  console.log(`> Description file loaded. It has ${Object.keys(definitions).length} rows.`);
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
    if (!simpleDefs[refSetId]) console.log(`No description for refset with id: ${refSetId}`);
    else {
      let simpleRefSets =
        refSets[refSetId].activeConcepts.length + refSets[refSetId].inactiveConcepts.length < 10000
          ? simpleRefSetsLT10000
          : simpleRefSets10000PLUS;
      const def = simpleDefs[refSetId].term;
      if (simpleRefSets[def]) console.log(`There is already an entry for: ${def}`);
      else {
        simpleRefSets[def] = refSets[refSetId];
      }
    }
  });

  // Find snomed codes without definition

  // First get the lookup of unknown codes
  const knownCodeLookup = existsSync(CODE_LOOKUP)
    ? JSON.parse(readFileSync(CODE_LOOKUP, "utf8"))
    : {};

  const unknownCodes = Object.values(simpleRefSetsLT10000)
    .map((x) => x.activeConcepts.concat(x.inactiveConcepts))
    .flat()
    .filter((conceptId) => !simpleDefs[conceptId])
    .map((conceptId) => {
      if (knownCodeLookup[conceptId]) {
        simpleDefs[conceptId] = knownCodeLookup[conceptId];
        return false;
      }
      return conceptId;
    })
    .filter(Boolean);

  if (unknownCodes.length > 0) {
    console.log(`> There are ${unknownCodes.length} codes without a definition.`);
    console.log(`> Attempting to look them up in the NHS SNOMED browser...`);
  }

  async function process40UnknownConcepts(items) {
    console.log(`Looking up next 40 (out of ${items.length})`);
    const next40 = items.splice(0, 40);
    const fetches = next40.map((x) => {
      return fetch(
        `https://termbrowser.nhs.uk/sct-browser-api/snomed/uk-edition/v20230927/concepts/${x}`
      ).then((x) => x.json());
    });
    const results = await Promise.all(fetches).catch((err) => {
      console.log(
        "Error retrieving data from NHS SNOMED browser. Rerunning will probably be fine."
      );
      process.exit();
    });
    results.forEach(({ conceptId, fsn, effectiveTime, active }) => {
      const def = {
        active,
        term: fsn,
        effectiveTime,
        isSynonym: false,
      };
      knownCodeLookup[conceptId] = def;
      simpleDefs[conceptId] = def;
    });
    writeFileSync(CODE_LOOKUP, JSON.stringify(knownCodeLookup, null, 2));
    const next = 2000 + Math.random() * 5000;
    if (items.length > 0) {
      console.log(`Waiting ${next} milliseconds before next batch...`);
      return new Promise((resolve) => {
        setTimeout(async () => {
          await process40UnknownConcepts(items);
          return resolve();
        }, next);
      });
    }
  }

  if (unknownCodes.length > 0) {
    await process40UnknownConcepts(unknownCodes);
  }

  const simpleDefsLT10000 = {};
  const simpleDefs10000PLUS = {};

  Object.values(simpleRefSetsLT10000)
    .map((x) => x.activeConcepts.concat(x.inactiveConcepts))
    .flat()
    .forEach((conceptId) => {
      simpleDefsLT10000[conceptId] = simpleDefs[conceptId];
    });

  Object.values(simpleRefSets10000PLUS)
    .map((x) => x.activeConcepts.concat(x.inactiveConcepts))
    .flat()
    .forEach((conceptId) => {
      if (simpleDefs[conceptId]) simpleDefs10000PLUS[conceptId] = simpleDefs[conceptId].term;
    });

  writeFileSync(definitionFile1, JSON.stringify(simpleDefsLT10000, null, 2));
  writeFileSync(definitionFile2, JSON.stringify(simpleDefs10000PLUS, null, 2));
  writeFileSync(refSetFile1, JSON.stringify(simpleRefSetsLT10000, null, 2));
  writeFileSync(refSetFile2, JSON.stringify(simpleRefSets10000PLUS, null, 2));

  return dir;
}

function brot(file, fileBrotli) {
  console.log(`> Compressing ${file}...`);
  const result = compress(readFileSync(file), {
    extension: "br",
    quality: 11, //compression level - 11 is max
  });
  console.log(`> Compressed. Writing to ${fileBrotli}...`);
  writeFileSync(fileBrotli, result);
}

function compressJson(dir) {
  const {
    definitionFile1,
    definitionFile2,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli1,
    definitionFileBrotli2,
    refSetFile1Brotli,
    refSetFile2Brotli,
  } = getFileNames(dir);
  if (
    existsSync(definitionFileBrotli1) &&
    existsSync(definitionFileBrotli2) &&
    existsSync(refSetFile1Brotli) &&
    existsSync(refSetFile2Brotli)
  ) {
    console.log(`> The brotli files already exist so I'll move on...`);
    // const definitions = JSON.parse(readFileSync(definitionFile));
    // const refSets = JSON.parse(readFileSync(definitionFile));
    return dir;
  }

  console.log("> Starting compression. TAKES A WHILE - GO GET A CUP OF TEA!");

  brot(refSetFile1, refSetFile1Brotli);
  brot(refSetFile2, refSetFile2Brotli);
  brot(definitionFile1, definitionFileBrotli1);
  brot(definitionFile2, definitionFileBrotli2);
  console.log(`> All compressed.`);
  return dir;
}

function rest() {
  const additionalRoutes = readFileSync(path.join(__dirname, "_additional_routes.txt"), "utf8")
    .split("\n")
    .map((x) => x.trim());
  const processedVersions = readdirSync(PROCESSED_DIR).filter((x) => x !== ".gitignore");
  const versions = additionalRoutes.concat(processedVersions);
  writeFileSync(path.join(__dirname, "web", "routes.json"), JSON.stringify(versions, null, 2));
}

import { S3Client, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

let s3;
async function uploadToS3(file, brotliFile) {
  const posixFilePath = file.split(path.sep).join(path.posix.sep);
  const params = {
    Bucket: "nhs-drug-refset",
    Key: posixFilePath,
  };

  const exists = await s3
    .send(new HeadObjectCommand(params))
    .then((x) => {
      console.log(`> ${file} already exists in R2 so skipping...`);
      return true;
    })
    .catch((err) => {
      if (err.name === "NotFound") return false;
    });

  if (!exists) {
    console.log(`> ${file} does not exist in R2. Uploading...`);
    await s3.send(
      new PutObjectCommand({
        Bucket: "nhs-drug-refset",
        Key: posixFilePath,
        Body: readFileSync(brotliFile),
        ContentEncoding: "br",
        ContentType: "application/json",
      })
    );
    console.log("> Uploaded.");
  }
}

async function uploadToR2(dir) {
  const accessKeyId = `${process.env.ACCESS_KEY_ID}`;
  const secretAccessKey = `${process.env.SECRET_ACCESS_KEY}`;
  const endpoint = `https://${process.env.ACCOUNT_ID}.r2.cloudflarestorage.com`;

  const {
    definitionFile1,
    definitionFile2,
    refSetFile1,
    refSetFile2,
    definitionFileBrotli1,
    definitionFileBrotli2,
    refSetFile1Brotli,
    refSetFile2Brotli,
  } = getFileNames(dir, true);

  s3 = new S3Client({
    region: "auto",
    credentials: {
      accessKeyId,
      secretAccessKey,
    },
    endpoint,
  });
  await uploadToS3(definitionFile1, definitionFileBrotli1);
  await uploadToS3(definitionFile2, definitionFileBrotli2);
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
