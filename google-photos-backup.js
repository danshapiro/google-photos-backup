/*
Description:
This script ensures an organized, metadata-enriched photo collection by automating backup processes, securing digital memories both locally and in the cloud, and maintaining an up-to-date database of photo backups.
- Automate photo URL scraping from Google Photos (scrapeAllGooglePhotosUrls)
- Scan all local images (scanDirectory)
- Utilize playwright-extra for browser automation and sqlite3 for robust data management (chromium, sqlite3)
- Read and write photo metadata directly to files or to sidecar files, based on user preferences (writeMetadataToFile, readMetadataFromFile)
- Support maximum photo limits, enable full disk rescans, facilitate incremental Google Photos backups, and allow for drop-and-create database table options (main, setupDatabase)
- Offer customizable settings for headless browsing, download paths, database initialization, and metadata writing options (main)
- Implement notification system for backup completion and errors (logSkippedEntry)
- For a given google photos image, search the db to see if there's a file on-disk whose metadata has the google photos URL in it, indicating that it's already been backed up (checkIfAllUrlsAreBackedUp)
- Include options for data deduplication to avoid backing up duplicates (handleDuplicateFiles)

High level functional overview:
- main() is the entry point, orchestrating the entire backup process. From here, the execution branches out to several key functions, each performing a distinct part of the workflow. The auxiliary functions are called by these key functions as needed.
- setupDatabase() prepares the database for operation. This function doesn't directly call any auxiliary functions but sets up the foundation for data storage and retrieval.
- scrapeAllGooglePhotosUrls(maxPics, db) is called if new Google Photos URLs need to be scraped. It does not directly call auxiliary functions within its description but interacts with the database and the browser session to scrape URLs.
- scanDirectory(startPath, db) Run if scrapeFiles is True. Invoked to scan the local filesystem for images. It calls:
  - addFileToDb(absolutePath, db) for each image found, to add their paths and metadata to the database. addFileToDb then calls:
    - readMetadataFromFile(filePath) to extract metadata from each image or its sidecar file, which is used to populate database entries.
- checkIfAllUrlsAreBackedUp(db, thoroughness) is executed to update the backup status of URLs in the database. It uses:
  - updateUrlBackupStatus(url, db) to check if a URL is backed up and update its status accordingly.
- incrementalBackup(db) is performed to download new photos from Google Photos and update their backup status. It involves several steps and auxiliary functions:
- navigateToUrl(page, url) to navigate to a photo's URL.
  - downloadPhoto(page, db) to manage the photo download process, which then calls:
    - openInfoPanelIfClosed(page) to ensure the photo's information panel is open for metadata extraction.
    - dispatchMouseEvents(page, buttonSelector) to simulate mouse events for clicking the download button.
    - downloadToTempLocation(page) to handle the actual download operation.
    - writeMetadataToFile(page, finalPath, people, url, dateTimeOriginal, description, writeSidecarMetadata, writeFileMetadata) to write or update the photo's metadata in the file or a sidecar file.
- Finally, browser.close() is called to close the browser session once all operations are completed.*/

import { chromium } from 'playwright-extra';
import path from 'path';
import sqlite3 from 'sqlite3';
import { moveFile } from 'move-file'
import { open } from 'sqlite';
import { exiftool } from 'exiftool-vendored';
import { existsSync } from 'fs';
import fs from 'fs';
import stealth from 'puppeteer-extra-plugin-stealth';
import fsP from 'node:fs/promises';
import { DateTime } from 'luxon';
import StreamZip from 'node-stream-zip';

const userDataDir = './session';
const downloadPath = './download'
const logErrorName = 'runtime-errors.log'
const writeSidecarMetadata = true; //write the metadata to an XMP file
const writeFileMetadata = true;  //modify the file to add metadata via EXIF
const maxPics = 0; //For testing. End after grabbing this many URLs (0 for no max)
const dropAndCreateFileTable = false; //will fully rescan the disk for backups. Takes ~1h.
const dropAndCreatePhotosTable = false; //setting to true means you will need to re-scrape the entire google photos collection, which is very slow (~1 day). Only way to pick up insertions where the date is not recent, though.
const scrapeGooglePhotos = true; // if true, looks for new images on google photos. Checks the latest images, gives up when it finds several photos that are backed up alreaady, so it's quick. Use with dropAndCreatePhotosTable if you want to rebuild entirely. 
const scrapeFiles = false; // if true, looks for which images are on disk. Not incremental, fully rescans every time, but only adds, not change/remove. Use with dropAndCreateFileTable=true to fully update. 
const performIncrementalBackup = true; //Back up all the new URLs to disk. Usually do this.
const debug = true; //halts on errors, writes out page html on each page to disk for debugging, etc
const checkBackupThoroughness = 'unchecked' // 'unchecked', 'unchecked-and-no', 'all'
const validFileExtensions = ['.avif', '.bmp', '.dng', '.gif', '.heic', '.ico', '.jpg', '.jpeg', '.png', '.tif', '.tiff', '.webp', '.raw', '.3gp', '.3g2', '.asf', '.avi', '.divx', '.m2t', '.m2ts', '.m4v', '.mkv', '.mmv', '.mod', '.mov', '.mp4', '.mpg', '.mts', '.nef', '.tod', '.wmv', '.zip'];

import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';

function setupLogging() {
  const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf(info => `${info.timestamp} - ${info.level} - ${info.message}`)
    ),
    transports: [
      new DailyRotateFile({
        filename: 'logs/google-photos-backup-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: false,
        maxSize: '20m',
        maxFiles: '10',
        prepend: true, // Deprecated in newer versions, included for reference
        level: 'debug'
      }),
      new winston.transports.File({
        filename: 'logs/' + logErrorName,
        level: 'error'
      }),
      new winston.transports.Console({
        level: 'info',
        format: winston.format.simple()
      }),
    ],
  });

  return logger;
}

// Let's log
const logger = setupLogging();
logger.error('Logger started.\n\n\n'); //"error" level so that it shows up in both logs

export { setupLogging }; // in case other modules want to use this later
//record total runtime
const startTime = Date.now();

chromium.use(stealth());
let browser = null; // Change const to let to allow reassignment

async function launchBrowser() {
  browser = await chromium.launchPersistentContext(path.resolve(userDataDir), { 
    headless: false,
    acceptDownloads: true,
    channel: 'chrome', // possible values: chrome, msedge, and chromium
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
}
// Add all photos from google photos to the db.
// Go to the google photos homepage, keep clicking right arrow to select the next image, and stash each URL in the db.
async function scrapeAllGooglePhotosUrls(maxPics, db) {
  const page = await browser.newPage();
  try {
    await page.goto('https://photos.google.com');
  } catch (error) {
    logger.error(`Failed to navigate to https://photos.google.com. Error: ${error}`);
    throw error; // Re-throw the error to handle it in the calling function
  }
  // Check if redirected to google.com/photos/about
  if (page.url() === 'https://www.google.com/photos/about/') {
    throw new Error("You don't appear to be logged in.\nFrom the command line, run 'node setup.js' in this directory. Log in, then close the browser. Then run this again.");
    return;
    }

  logger.info('Navigated to Google Photos');

  let currentUrl = '';
  let previousUrl = '';
  let imageUrlsScraped = 0;
  let sameUrlCount = 0;
  const sameUrlCountMax = 5 //abort if you see the same URL this number of times in a row
  let alreadyBackedUpCount = 0;
  const alreadyBackedUpCountMax = 10; //abort when you see this many photos in a row that are already backed up

  do {
    await page.keyboard.press('ArrowRight')
    await page.waitForTimeout(100); // wait for the new image to load
    const imageUrl = await page.evaluate(() => document.activeElement.toString())
    logger.info(`Pic: ${imageUrl}`);
    // Check if imageUrl starts with 'https://photos.google.com/photo/'
    const url = new URL(imageUrl);
    if (!(url.host === 'photos.google.com' && url.pathname.startsWith('/photo/'))) {
      logger.error(`Invalid URL: ${imageUrl}`);
      debugger;
      continue; // Skip to the next iteration of the loop
    }

    // Add to the database
    const timestamp = new Date().toISOString();
    try {
      await db.run(`INSERT INTO googlePhotosUrls(url, isBackedUp, retrieved_on) VALUES(?, 'unchecked', ?)`, [imageUrl, timestamp]);
    } catch (err) {
      if (err.code === 'SQLITE_CONSTRAINT') {
        logger.debug(`Already backed up.`);
        alreadyBackedUpCount++;
      } else {
        logger.error(`SQL Error: ${err}`);
        debugger;
      }
    }
    // Update the previous URL and increment the image index
    previousUrl = currentUrl;
    currentUrl = imageUrl;
    imageUrlsScraped++;

    // If the previous URL is the same as the current one, increment sameUrlCount
    if (previousUrl === currentUrl) {
      sameUrlCount++;
    } else {
      sameUrlCount = 0; // reset sameUrlCount if the URLs are different
    }

    // If a maximum number of images is specified and we've reached it, break the loop
    if (maxPics && imageUrlsScraped >= maxPics) {
      logger.info(`Reached maximum number of images: ${maxPics}`);
      break;
    }
  } while (sameUrlCount < sameUrlCountMax && alreadyBackedUpCount < alreadyBackedUpCountMax);
};

// Go to a URL and wait for it to load
async function navigateToUrl(page, url) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' }); // Wait until the network is idle
    return true; // Return true if the navigation is successful
  } catch (error) {
    logger.error(`Error navigating to URL: ${url}. Error: ` + error);
    return false; // Return false if there is an error
  }
};

// Add a DB operation to the queue
const queue = [];
let isProcessing = false;
const batchSize = 1000;
async function addToDbQueue(sql, params, db) {
  if (queue.length % 100 === 0 && queue.length !== 0) {
    logger.info(`Added. Current queue size: ${queue.length}`);
  }
  return new Promise((resolve) => {
    queue.push({ sql, params, resolve });

    if (!isProcessing) {
      processDbQueue(db);
    }
    resolve(true);
  });
};

// Send a queued operation to the db
async function processDbQueue(db) {
  if (queue.length % 100 === 0 && queue.length !== 0) {
    logger.info(`Removing. Current queue size: ${queue.length}`);
  }

  if (queue.length === 0) {
    isProcessing = false;
    return;
  }

  isProcessing = true;
  const tasks = [];
  const batch = [];
  for (let i = 0; i < batchSize && queue.length > 0; i++) {
    const { sql, params, resolve } = queue.shift();
    batch.push({ sql, params });
    tasks.push(resolve);
  }

  for (const { sql, params } of batch) {
    try {
      await db.run(sql, params);
    } catch (err) {
      if (err.code === 'SQLITE_CONSTRAINT') {
        logger.debug(`Duplicate entry ignored: ${params[0]}`);
      } else {
        logger.error(`SQL Error: ${err}`);
        debugger;
      }
    }
  }

  tasks.forEach(resolve => resolve());
  processDbQueue(db);
};

// Wait for all db operations to finish before returning
function waitForQueueToDrain() {
  return new Promise(resolve => {
    const checkQueue = () => {
      if (queue.length === 0 && !isProcessing) {
        logger.info('Queue is drained to empty.');
        resolve();
      } else {
        setTimeout(checkQueue, 100);
      }
    };
    checkQueue();
  });
};

function convertExifDateTimeToISO(dateToConvert) {
  if (!dateToConvert) {
    return ''
  }
  let DateISO = `${dateToConvert.year}-${dateToConvert.month.toString().padStart(2, '0')}-${dateToConvert.day.toString().padStart(2, '0')}T${dateToConvert.hour.toString().padStart(2, '0')}:${dateToConvert.minute.toString().padStart(2, '0')}:${dateToConvert.second.toString().padStart(2, '0')}`;

  if (dateToConvert.tzoffset !== undefined) {
    // Convert timezone offset from minutes to HH:mm format
    let offsetHours = Math.floor(Math.abs(dateToConvert.tzoffset) / 60);
    let offsetMinutes = Math.abs(dateToConvert.tzoffset) % 60;
    let sign = dateToConvert.tzoffset < 0 ? '-' : '+';
    DateISO += `${sign}${offsetHours.toString().padStart(2, '0')}:${offsetMinutes.toString().padStart(2, '0')}`;
  } else {
    // Assume UTC if no timezone offset is provided
    DateISO += 'Z';
  }
  return DateISO;
}

//return the metadata found in the image file or in an xmp sidecar file
async function readMetadataFromFile(filePath) {
  const xmpFilePath = `${filePath}.xmp`;

  let metadata;
  if (fs.existsSync(xmpFilePath)) {
    metadata = await exiftool.read(xmpFilePath);
  } else {
    logger.info(`XMP file not found for ${filePath}, reading EXIF data instead.`);
    metadata = await exiftool.read(filePath);
  }

  let url = metadata['Source'];
  let createDate = convertExifDateTimeToISO(metadata['CreateDate']);
  let dateTimeOriginal = convertExifDateTimeToISO(metadata['DateTimeOriginal']);
  let description = metadata['Description'] || metadata['XMP:Description'];
  let personInImage = '';
  if (metadata['PersonInImage']) {
      personInImage = metadata['PersonInImage'][0];
  } else if (metadata['XMP:PersonInImage']) {
      personInImage = metadata['XMP:PersonInImage'][0];
  }

  if (!url) {
    logger.error(`URL not found in metadata for ${filePath}`);
    return;
  }

  return { url, createDate, dateTimeOriginal, description, personInImage };
};

// Add an on-disk file to the database along with its metadata
async function addFileToDb(absolutePath, db) {
  // Check if the file extension is in the list of allowed extensions
  const ext = path.extname(absolutePath).toLowerCase();
  if (!validFileExtensions.includes(ext)) {
    logger.error(`Found an invalid file type: ${absolutePath}`);
    debugger;
    return;
  }
  const metadata = await readMetadataFromFile(absolutePath);
  if (metadata) {
    addToDbQueue(`INSERT INTO files(path, url, createDate, dateTimeOriginal, description, personInImage, last_updated_date) VALUES(?, ?, ?, ?, ?, ?, ?)`,
      [absolutePath, metadata.url, metadata.createDate, metadata.dateTimeOriginal, metadata.description, metadata.personInImage, new Date().toISOString()], db);
  } else {
    logger.error(`No metadata found for file: ${absolutePath}`);
    //    debugger; //TODO: What do we do when we find an image that was put there manually, with no metadata to tie it back to a google photos url?
  }
}

// Scan recursively for files and add them plus their XMP metadata to the db
async function scanDirectory(startPath, db) {
  logger.info(`Processing directory: ${startPath}`);
  const list = await fs.promises.readdir(startPath);
  const filePromises = [];
  const directoryPromises = [];

  for (const file of list) {
    const absolutePath = path.join(startPath, file);
    const stat = await fs.promises.stat(absolutePath);

    if (stat.isDirectory()) {
      // Store the promise for processing the subdirectory later
      directoryPromises.push(() => scanDirectory(absolutePath, db));
    } else if (!absolutePath.endsWith('.xmp')) {
      // Process files concurrently within the same directory
      filePromises.push(addFileToDb(absolutePath, db));
    }
  }
  // Wait for all file processing in the current directory to complete
  await Promise.all(filePromises);
  // Now process each subdirectory one at a time, waiting for each to complete before moving to the next
  for (const directoryPromise of directoryPromises) {
    await directoryPromise();
  }
};

// Check if there is a file in the database whose URL field matches the specified url
async function updateUrlBackupStatus(url, db, downloadFailed) {
  if (downloadFailed) {
    logger.debug(`Image URL ${url} failed to download. Setting db status to error.`);
    addToDbQueue(`UPDATE googlePhotosUrls SET isBackedUp = 'error' WHERE url = ?`, [url], db);
    return
  }
  const fileRow = await db.get(`SELECT * FROM files WHERE url = ?`, [url]);
  if (fileRow) {
    logger.debug(`Image URL ${url} is backed up in file ${fileRow.path}`);
    addToDbQueue(`UPDATE googlePhotosUrls SET isBackedUp = 'yes' WHERE url = ?`, [url], db);
  } else {
    addToDbQueue(`UPDATE googlePhotosUrls SET isBackedUp = 'no' WHERE url = ?`, [url], db);
  }
};

// Go through all photo URLs from google photos and see if there's a file in the db that has its URL in the source field, (indicating the photo is already backed up to disk)
async function checkIfAllUrlsAreBackedUp(db, thoroughness = 'unchecked') {
  try {
    let query;
    switch (thoroughness) {
      case 'all':
        query = `SELECT url FROM googlePhotosUrls`;
        break;
      case 'unchecked-and-no':
        query = `SELECT url FROM googlePhotosUrls WHERE isBackedUp IN ('unchecked', 'no')`;
        break;
      case 'unchecked':
      default:
        query = `SELECT url FROM googlePhotosUrls WHERE isBackedUp = 'unchecked'`;
        break;
    }
    const rows = await db.all(query);
    logger.debug(`About to check backup status for ${rows.length} items.`);
    let completedCount = 0;
    for (const row of rows) {
      const photosUrl = row.url;
      await updateUrlBackupStatus(photosUrl, db, false);
      completedCount++;
      const percentComplete = Math.floor((completedCount / rows.length) * 100);
      if (percentComplete % 2 === 0) {
        logger.info(`${percentComplete}% complete.`);
      }
    }
  } catch (err) {
    logger.error('checkBackup SQL error:', err);
    debugger;
  }
};

// Find a selector on the page and return its visibility and enabled status
async function findActiveSelector(page, selector) {
  const element = await page.$(selector);
  if (element) {
    const isVisible = await page.evaluate(el => el.offsetParent !== null, element);
    const isEnabled = !(await element.isDisabled());
    if (isVisible && isEnabled) {
      return true;
    } else {
      logger.error(`Element found but status is visible: ${isVisible}, enabled: ${isEnabled}, for selector: ${selector}`);
      return false;
    }
  }
//  logger.info(`Element not found: ${selector}`);
  return false;
}

// Wait for a selector on the page to become visible and enabled, up to a specified timeout
async function waitForActiveSelector(page, selector, timeout = 5000) {
  try {
    await page.waitForSelector(selector, { state: 'attached', timeout });
    const element = await page.$(selector);
    if (element) {
      const isVisible = await page.evaluate(el => el.offsetParent !== null, element);
      const isEnabled = !(await element.isDisabled());
      if (isVisible && isEnabled) {
        return true;
      } else {
        logger.error(`Element is not active. Visible: ${isVisible}, Enabled: ${isEnabled}, Selector: ${selector}`);
        return false;
      }
    }
  } catch (error) {
    logger.error(`waitForActiveSelector error: Selector not found or timeout exceeded for ${selector}. Error: ${error}`);
    return false;
  }
}

// When the info panel is closed, open it
async function openOptionsPanel(page) {
  const selectorToLookFor = '[aria-label="Download - Shift+D"]';
  let isOptionsPanelOpen = await findActiveSelector(page, selectorToLookFor);
  
  if (!isOptionsPanelOpen) {
    try {
      await dispatchMouseEvents(page, 'div[data-tooltip="More options"]');
      await page.waitForTimeout(2000); // It seems to need a hard wait, perhaps because of the animation? The next line doesn't do it.
      isOptionsPanelOpen = await waitForActiveSelector(page, selectorToLookFor, 5000);
      if (isOptionsPanelOpen) {
        return true;
      } else {
        logger.error(`Options panel failed to open.`);
        debugger;
        return false;
      }
    } catch (error) {
      logger.error(`Failed to open 'More options': ${error}`);
      debugger;
    }
  }
}

// When the info panel is closed, open it
async function openInfoPanelIfClosed(page) {
  let isInfoPanelOpen;
  const closeButtonSelector = 'div.IMbeAf'; // This is the best selector to look for to see if it's open

  // Set a timeout for the entire operation
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Operation timed out')), 20000); // 20 seconds
  });

  //see if it's open already
  try {
    await Promise.race([
      (async () => {
        await page.waitForSelector(closeButtonSelector, { state: 'attached', timeout: 5000 });
        isInfoPanelOpen = true;
      })(),
      timeoutPromise
    ]);
  } catch (error) {
    logger.debug(`Info panel is closed (did not find selector): ${error}`);
    isInfoPanelOpen = false;
  }
  if (!isInfoPanelOpen) {
    logger.debug("Couldn't open it with the selector. Try pressing 'i'.")
    try {
      await Promise.race([
        (async () => {
          await page.keyboard.press('i');
          console.log('Pressed i to open info panel')
          await page.waitForSelector(closeButtonSelector, { state: 'attached', timeout: 5000 }); // wait 5s
        })(),
        timeoutPromise
      ]);
    } catch (error) {
      logger.error(`Error opening the info panel: ${error}`);
      return false;
    }
  }
  return true;
}

// Download the photo to a temp location, get its metadata, check if it's a duplicate, write the file and metadata to disk, then add the file on disk to the db.
async function downloadPhoto(page, db) {
  const timeout = 120000; // 2 minutes in milliseconds
  let timeoutHandle;
  const currentPageUrl = page.url(); 

  // Set up a promise that rejects after a timeout
  const timeoutPromise = new Promise((resolve) => { 
    timeoutHandle = setTimeout(() => {
      logger.error(`Download photo operation timed out. URL: ${currentPageUrl}`);
      resolve({timeoutOccurred: true}); 
    }, timeout);
  });

  // Use Promise.race to handle the timeout
  const operationPromise = (async () => {
    let infoPanelOpen = await openInfoPanelIfClosed(page);
      if (!infoPanelOpen) {
        logger.error(`The info panel couldn't be opened; something's awry. Move on to the next photo. URL: ${currentPageUrl}`);
        return {processedSuccessfully: false, timeoutOccurred: false}; 
      }

    let downloadInfos;
    try {
      downloadInfos = await downloadToTempLocation(page);
    } catch (error) {
      logger.error(`Error downloading photo: ${error}. URL: ${currentPageUrl}`);
      return {processedSuccessfully: false, timeoutOccurred: false}; 
    }

    let processedSuccessfully = true;
    for (const { download, tempDownloadPath } of downloadInfos) {
      const fileNamePhoto = await download.suggestedFilename();
      const fileExtensionPhoto = path.extname(fileNamePhoto).toLowerCase();

      if (!validFileExtensions.includes(fileExtensionPhoto)) {
        logger.error(`Invalid file extension for ${fileNamePhoto}. Skipping. URL: ${currentPageUrl}`);
        debugger;
        processedSuccessfully = false; // Mark as unsuccessful due to invalid file extension
        continue;
      }

      if (fileExtensionPhoto === '.zip') {
        const zipProcessingResult = await processZipFile(db, tempDownloadPath, page);
        processedSuccessfully = processedSuccessfully && zipProcessingResult;
      } else {
        const singleFileProcessingResult = await processSingleFile(db, tempDownloadPath, fileNamePhoto, page);
        processedSuccessfully = processedSuccessfully && singleFileProcessingResult;
      }
    }
    return {processedSuccessfully, timeoutOccurred: false}; 
  })();

  const result = await Promise.race([operationPromise, timeoutPromise]).finally(() => {
    clearTimeout(timeoutHandle); // Ensure the timeout is cleared in any case
  });
  return result;
}

async function processZipFile(db, zipFilePath, page) {
  const zip = new StreamZip.async({ file: zipFilePath });
  const entries = await zip.entries();
  let processedSuccessfully = false;

  for (const entry of Object.values(entries)) {
    if (entry.isDirectory) continue;
    const ext = path.extname(entry.name).toLowerCase();
    if (!validFileExtensions.includes(ext)) continue;

    const tempDownloadPath = path.join(path.dirname(zipFilePath), entry.name);
    await zip.extract(entry.name, tempDownloadPath);
    const processResult = await processSingleFile(db, tempDownloadPath, entry.name, page);
    processedSuccessfully = processedSuccessfully || processResult;
  }

  await zip.close();
  await fs.promises.unlink(zipFilePath); // Clean up the ZIP file after processing
  return processedSuccessfully;
}

async function processSingleFile(db, tempDownloadPath, filename, page) {
  const metadata = await extractMetadataFromPage(page, tempDownloadPath, filename);
  if (!metadata) {
    logger.error(`Failed to extract metadata for ${filename}.`);
    return false;
  }

  const { year, month, dateTimeOriginal, people, description } = metadata;
  let finalPath = path.join(downloadPath, year.toString(), month.toString(), filename);

  finalPath = await handleDuplicateFiles(finalPath, tempDownloadPath, page, filename, year, month);
  await writeMetadataToFile(page, finalPath, people, page.url(), dateTimeOriginal, description, writeSidecarMetadata, writeFileMetadata);
  
  await addFileToDb(finalPath, db);

  return true;
}

// Dispatch mouse events to the first clickable and visible selector found (in case of multiples).
async function dispatchMouseEvents(page, buttonSelector) {
  try {
    // Find all buttons matching the selector
    const buttons_matching_selector = await page.$$(buttonSelector);

    // Find the first visible and enabled button
    let actionableButton;
    for (let button_match of buttons_matching_selector) {
      let visible = await page.evaluate(el => el.offsetParent !== null, button_match);
      let enabled = !(await button_match.isDisabled());
      if (visible && enabled) {
        actionableButton = button_match;
        break;
      }
    }

    // If a visible and enabled button is found, dispatch mouse events
    if (actionableButton) {
      await page.evaluate((button_on_page) => {
        button_on_page.dispatchEvent(new MouseEvent('mousedown', {
          'view': window,
          'bubbles': true,
          'cancelable': true
        }));
        button_on_page.dispatchEvent(new MouseEvent('mouseup', {
          'view': window,
          'bubbles': true,
          'cancelable': true
        }));
      }, actionableButton);
    } else {
      logger.error(`No visible and enabled button found for selector: ${buttonSelector}`);
      debugger;
    }
  } catch (error) {
    logger.error('Error simulating mousedown and mouseup events: ' + error);
    debugger;
  }
}

// Open More Options, try to download all versions available, return the path to the temp download location
async function downloadToTempLocation(page) {

  // Initialize an array to hold promises for each download event
  const downloadPromises = [];
  let downloadInfos = [];

  await page.waitForTimeout(1000); // The page seems to need 1s to finish rendering before we click

  /// Define the various download buttons
  const downloadSelectors = [
    '[aria-label^="Download all "]',
    '[aria-label="Download original"]',
    '[aria-label="Download video"]',
    '[aria-label="Download - Shift+D"]',
  ];

  // Iterate over each selector to handle multiple downloads
  for (const selector of downloadSelectors) {
    await openOptionsPanel(page);
    const dlButtons = await page.$$(selector);
    if (dlButtons.length > 0) {
      logger.info(`Downloading from: ${selector}`);
      // Click the button to trigger the download
      await dispatchMouseEvents(page, selector);

      // Prepare to wait for the download event
      const downloadPromise = page.waitForEvent('download', { timeout: 60000 }).then(download => {
        return download.path().then(path => ({ download, tempDownloadPath: path }));
      }).catch(error => {
        logger.error(`Error waiting for download completion: ${error}`);
        debugger;
      });
      // Add the promise to the array
      downloadPromises.push(downloadPromise);
      // Wait for the download to finish
      await downloadPromise;
    }
  }
  if (downloadPromises.length === 0) {
    logger.error('Did not download successfully.');
    debugger;
  }
  // Wait for all download promises to resolve
  try {
    downloadInfos = await Promise.all(downloadPromises);
  } catch (error) {
    logger.error(`There was an error while downloading: ${error}`);
    debugger;
  }

  // Filter out any undefined or null results
  return downloadInfos.filter(info => info && info.tempDownloadPath);
}

// Examine the page hosting the image and return the metadata that's shown onscreen
async function extractMetadataFromPage(page, tempDownloadPath, filename) {
  const exifData = await exiftool.read(tempDownloadPath);
  let year = exifData.DateTimeOriginal?.year || 1;
  let month = exifData.DateTimeOriginal?.month || 1;
  let dateTimeOriginal = exifData.DateTimeOriginal;

  const metadata = await page.evaluate(() => {
    const data = {
      people: null,
      detailsHtml: null,
      description: null
    };

    // Find all "People" sections and extract names if visible
    const peopleSections = Array.from(document.querySelectorAll('div.wiOkb')).filter(element => element.textContent === 'People');
    const visiblePeopleSection = peopleSections.find(section => section.offsetParent !== null);
    if (visiblePeopleSection) {
      const parentElement = visiblePeopleSection.parentElement;
      data.people = Array.from(parentElement.querySelectorAll('ul.cdLvR li a[aria-label]')).map(element => element.getAttribute('aria-label'));
    }

    // Get the HTML of the whole details panel to use for the date
    const detailsSection = Array.from(document.querySelectorAll('div.wiOkb')).find(element => element.textContent === 'Details');
    if (detailsSection) {
      const parentElement = detailsSection.parentElement;
      data.detailsHtml = parentElement.innerHTML;
    }

    // Grab the first 'Description' element that is visible
    const descriptionElement = document.querySelector('textarea[aria-label="Description"]');
    if (descriptionElement && descriptionElement.offsetParent !== null) {
      data.description = descriptionElement.value;
    } else {
      data.description = "";
    }

    return data;
  });

  // Only search for a date if the file doesn't have its own date in EXIF
  if (year === 1 && month === 1 && metadata.detailsHtml != null) {
    const regex1 = new RegExp([
      'aria-label="',
      '[^"]*',
      '(Photo|Video|Animation|Highlight Video)',
      '[^"]*-',
      ' ([^"]+)',
      '"'
    ].join(''), 'g');
    const regex2 = new RegExp([
      'aria-label="Date:',
      '\\s*([A-Za-z0-9 ,]+)',
      '"'
    ].join(''), 'g');
    const match1 = regex1.exec(metadata.detailsHtml);
    const match2 = regex2.exec(metadata.detailsHtml);
    let dateString = match1 ? match1[1] : match2 ? match2[1] : null;

    if (dateString && !/\d{4}/.test(dateString)) {
      const currentYear = new Date().getFullYear();
      dateString += `, ${currentYear}`;
    }
    if (dateString) {
      const date = new Date(dateString);
      year = date.getFullYear();
      month = date.getMonth() + 1;
      dateTimeOriginal = date;
      logger.debug(`Extracted date from details: ${dateString}`);
    } else {
      logger.error('Could not find the date in the HTML details');
      debugger;
    }
  }

  if (isNaN(year) || isNaN(month) || year === 1) {
    logger.error(`Year or Month is not a valid number for URL: ${page.url()} and filename: ${filename}. Year: ${year}, Month: ${month}.`);
    debugger;
    return null;
  }

  logger.info(`Filename: ${filename}: on ${month}/${year}`)
  logger.info(`Date and Time: ${dateTimeOriginal ? dateTimeOriginal.toISOString() : 'Not Available'}`)
  if (metadata.people?.length > 0) logger.info(`People: ${metadata.people.join(', ')}`); 
  if (metadata.description?.length > 0) logger.info(`Description: ${metadata.description}`);

  return { fileName: filename, year, month, dateTimeOriginal, people: metadata.people, description: metadata.description };
}

// If a file is downloaded with the same name as an existing file, check to see if the existing file has the same source URL. If it does, overwrite the existing file with the new one. If it doesn't, write it out with a new filename by appending a number. 
async function handleDuplicateFiles(finalPath, tempDownloadPath, page, fileName, year, month) {
  let counter = 1;
  let fileExists = true;
  let newFileName; // Declare newFileName here

  while (fileExists) {
    try {
      await fsP.access(finalPath);
      let imageMetadata;
      let sourcePath = "";
      const sidecarFilePath = `${finalPath}.xmp`;

      if (existsSync(sidecarFilePath)) {
        imageMetadata = await exiftool.read(sidecarFilePath);
        sourcePath = imageMetadata['Source'];
      } else {
        imageMetadata = await exiftool.read(finalPath);
        sourcePath = imageMetadata['XMP:Source'];
      }

      if (sourcePath === page.url()) {
        logger.debug('Duplicate Detected with the same source as the URL of this image; Overwriting: '+finalPath+' from URL: '+page.url());
        await moveFile(tempDownloadPath, finalPath, { overwrite: true });
        fileExists = false;
      } else {
        logger.info(finalPath+' is a duplicate, but the gphotos source doesnt exist or is different from the URL of this page, so saving this as a separate file by appending a number.')
        newFileName = fileName.replace(/(\.[\w\d_-]+)$/i, `-${counter}$1`); 
        finalPath = path.join(downloadPath, year.toString(), month.toString(), newFileName);
        counter++;
      }
    } catch (error) {
      if (error.code === 'ENOENT') {
        await moveFile(tempDownloadPath, finalPath);
        logger.info('Adjusted Filename; Download Complete: '+ finalPath + ' URL:' + page.url());
        fileExists = false;
      } else {
        logger.error('Unexpected error while moving from temp to final location:', error);
        debugger;
      }
    }
  }
  return finalPath;
}

async function deleteFileIfExists(filePath) {
  if (existsSync(filePath)) {
    await fsP.unlink(filePath);
  }
}

async function get_page_html(page) {
  if (debug) {
    let page_html = `<!-- URL: ${page.url()} -->\n\n`;
    page_html += await page.content();
    try {
      await fsP.writeFile('debug_html.html', page_html);
    } catch (error) {
      logger.error(`Error writing to file: ${error}`);
      debugger;
    }
    return page_html;
  }
}

// Write the file's metadata to disk, either as a sidecar or to the file itself
async function writeMetadataToFile(page, finalPath, people, url, dateTimeOriginal, description, writeSidecarMetadata, writeFileMetadata) {
  logger.debug("Trying to write metadata to file: "+finalPath);
  const fileTypesWithoutExif = ['.avi', '.bmp', '.wmv', '.mts', '.dng', '.zip'];
  if (writeFileMetadata || writeSidecarMetadata) {
    try {
      // Convert the ExifDateTime object to a DateTime object
      const luxonDateTime = DateTime.fromObject({
        year: dateTimeOriginal.year,
        month: dateTimeOriginal.month,
        day: dateTimeOriginal.day,
        hour: dateTimeOriginal.hour,
        minute: dateTimeOriginal.minute,
        second: dateTimeOriginal.second
      });

      // Format the date to the correct format
      const formattedDate = luxonDateTime.toFormat("yyyy:LL:dd HH:mm:ss");

      const metadata = {
        'XMP:CreateDate': DateTime.now().toISO(),
        'XMP:Source': url,
        'DateTimeOriginal': formattedDate,
        'XMP:Description': description,
        'XMP:PersonInImage': people && people.length > 0 ? people.join(', ') : null
      };

      for (const [key, value] of Object.entries(metadata)) {
        if (value !== '' && value !== null) {
          //logger.info(`Writing metadata: ${key} = ${value}`);
          if (writeSidecarMetadata) {
            try {
              const sidecarFilePath = `${finalPath}.xmp`;
              await exiftool.write(sidecarFilePath, { [key]: value });
              await deleteFileIfExists(`${sidecarFilePath}_original`);
            } catch (sidecarError) {
              logger.error(`Error writing sidecar metadata for ${key} = ${value}: ${sidecarError}`);
              debugger;
            }
          }
          const lowercaseFilePath = finalPath.toLowerCase();
          if (writeFileMetadata && !fileTypesWithoutExif.some(ext => lowercaseFilePath.endsWith(ext))) {
            try {
              await exiftool.write(finalPath, { [key]: value });
              await deleteFileIfExists(`${finalPath}_original`);
            } catch (error) {
              logger.debug(`Error while writing file metadata ${key} = ${value}: ${error}. Attempting to rewrite all tags.`);
              try {
                const tempFilePath = `${finalPath}_temp.jpg`;
                await deleteFileIfExists(tempFilePath);
                await fsP.copyFile(finalPath, tempFilePath);
                await deleteFileIfExists(finalPath);
                await exiftool.rewriteAllTags(tempFilePath, finalPath);
                logger.debug(`Rewrote all tags successfully for ${finalPath}`);
                await deleteFileIfExists(tempFilePath);
              } catch (rewriteError) {
                logger.error(`Failed to rewrite all tags for ${finalPath}: ${rewriteError}`);
                // Check if tempFilePath exists and move it to finalPath if so
                try {
                  await fsP.access(tempFilePath, fs.constants.F_OK);
                  await fsP.rename(tempFilePath, finalPath);
                  logger.debug(`Moved ${tempFilePath} to ${finalPath} after failure to rewrite tags.`);
                } catch (accessError) {
                  logger.error(`Could not move ${tempFilePath} to ${finalPath}: ${accessError}`);
                }
                debugger;
              }
            }
          }
        }
      }
    } catch (error) {
      logger.error('Error modifying EXIF metadata for file. finalPath: ' + finalPath + ', URL: ' + page.url() + '.  Error: ' + error);
      debugger;
    }
  }
  logger.debug('Wrote metadata to file: ' + finalPath);
}

// Set up the database
async function setupDatabase() {
  const db = await open({
    filename: './google-photos-backup.db',
    driver: sqlite3.Database
  });
  // Drop the tables if they exist and instructed to
  if (dropAndCreateFileTable) {
    await db.run(`DROP TABLE IF EXISTS files`);
  }
  if (dropAndCreatePhotosTable) {
    await db.run(`DROP TABLE IF EXISTS googlePhotosUrls`);
  }
  // Create the tables
  await db.run(`CREATE TABLE IF NOT EXISTS googlePhotosUrls(url TEXT UNIQUE, isBackedUp TEXT DEFAULT 'unchecked', retrieved_on TEXT)`);
  await db.run(`CREATE TABLE IF NOT EXISTS files(path TEXT UNIQUE, url TEXT, createDate TEXT, dateTimeOriginal TEXT, description TEXT, personInImage TEXT, last_updated_date TEXT)`);
  return db;
};

// Once you've scraped disk + google photos, you can run this to incrementally back up new photos from google photos to disk.
async function incrementalBackup(db) {
  const maxRetries = 3;
  let photo = true;
  let downloadFailed;
  let page = await browser.newPage();
  while (photo) {
    // Retrieve the most recently scraped URL that hasn't been backed up yet
    photo = await db.get(`SELECT url FROM googlePhotosUrls WHERE isBackedUp = 'no' ORDER BY retrieved_on DESC LIMIT 1`);
    if (!photo) {
      break; // Exit the loop if there are no more photos to back up
    }
    logger.info(`Downloading: ${photo.url}`);
    var navigationSuccessful = await navigateToUrl(page, photo.url);
    if (navigationSuccessful) {
      downloadFailed = true; // Assume failure until proven otherwise
      for (let i = 0; i < maxRetries; i++) {
        const {processedSuccessfully, timeoutOccurred} = await downloadPhoto(page, db);

        if (processedSuccessfully) {
          downloadFailed = false;
          break; // Success, no need to retry
        } else {
          logger.error(`Issue encountered (timeout or other) for URL: ${photo.url}. Resetting browser and retrying (${i+1}/${maxRetries}).`);
          await browser.close();
          await launchBrowser();
          page = await browser.newPage();
          navigationSuccessful = await navigateToUrl(page, photo.url); // Reassign and test navigationSuccessful here
          if (!navigationSuccessful) {
            logger.error(`Failed to navigate to URL: ${photo.url} after retrying. Moving to the next photo.`);
            downloadFailed = true; // Ensure downloadFailed reflects navigation failure
            break; // Exit retry loop if navigation fails
          }
        }
      }
    } else {
      downloadFailed = true;
      logger.error(`Failed to navigate to URL: ${photo.url}. Skipping this photo.`);
      debugger;
    }
    // Check if the file exists in the db and mark the image as backed up if so. If it failed, mark it as error.
    await updateUrlBackupStatus(photo.url, db, downloadFailed);
  }
};


async function validateDatabase(db) {
  let isValid = true;

  // Helper function to perform validation checks
  async function performValidationCheck(query, errorMessage) {
    const row = await db.get(query);
    if (row) {
      logger.error(errorMessage);
      isValid = false;
    }
  }

  try {
    logger.info("Validating database");

    // googlePhotosUrls table checks
    await performValidationCheck(
      `SELECT * FROM googlePhotosUrls WHERE url NOT LIKE 'https://photos.google.com/%' LIMIT 1`,
      "Invalid URL found in googlePhotosUrls. Expected URLs to start with 'https://photos.google.com/'."
    );

    await performValidationCheck(
      `SELECT * FROM googlePhotosUrls WHERE isBackedUp NOT IN ('yes', 'no', 'error', 'unchecked') LIMIT 1`,
      "Invalid isBackedUp status found in googlePhotosUrls. Expected 'yes', 'no', or 'error'."
    );

    await performValidationCheck(
      `SELECT * FROM googlePhotosUrls WHERE strftime('%Y-%m-%dT%H:%M:%fZ', retrieved_on) IS NULL LIMIT 1`,
      "Invalid retrieved_on date found in googlePhotosUrls. Expected format is a valid ISO 8601 date."
    );

    // files table checks
    await performValidationCheck(
      `SELECT * FROM files WHERE path NOT LIKE 'download\\%' LIMIT 1`,
      "Invalid path found in files. Expected path to start with 'download\\'."
    );

    await performValidationCheck(
      `SELECT * FROM files WHERE url NOT LIKE 'https://photos.google.com/%' LIMIT 1`,
      "Invalid URL found in files. Expected URLs to start with 'https://photos.google.com/'."
    );

    await performValidationCheck(
      `SELECT * FROM files WHERE strftime('%Y-%m-%dT%H:%M:%fZ', createDate) IS NULL OR strftime('%Y-%m-%dT%H:%M:%fZ', dateTimeOriginal) IS NULL OR strftime('%Y-%m-%dT%H:%M:%fZ', last_updated_date) IS NULL LIMIT 1`,
      "Invalid date found in files for createDate, dateTimeOriginal, or last_updated_date. Expected format is a valid ISO 8601 date."
    );

  } catch (error) {
    logger.error(`Error during database validation: ${error}`);
    isValid = false;
  }

  logger.info(`Database validation complete. Database valid: ${isValid}`);
  if (!isValid) {
    debugger;
    throw new Error('Database validation failed.');
  }
  return isValid;
}

// Modify the main function to include try-catch-finally to handle normal completion and errors
async function main() {
  try {
    logger.info('Starting...');
    await launchBrowser();
    const db = await setupDatabase();
    await validateDatabase(db);

    if (scrapeGooglePhotos) {
      await scrapeAllGooglePhotosUrls(maxPics, db);
      await validateDatabase(db);
      await waitForQueueToDrain();
      logger.info('Scraping Google Photos to find new images - complete.')
    }
    if (scrapeFiles) {
      await scanDirectory(downloadPath, db);
      await validateDatabase(db);
      await waitForQueueToDrain();
      logger.info('Scanning directory for existing photos and metadata - complete.')
    }
    if (performIncrementalBackup) {
      await checkIfAllUrlsAreBackedUp(db, checkBackupThoroughness);
      await validateDatabase(db);
      await waitForQueueToDrain();
      await incrementalBackup(db);
      await validateDatabase(db);
    }
  } catch (error) {
    logger.error(`An error occurred during overall program operation: ${error}`);
  } finally {
    const endTime = Date.now();
    const timeElapsed = endTime - startTime;
    logger.info(`Script completed. Total time elapsed: ${timeElapsed} ms.`);
    await browser.close();
    process.exit(0); // Ensure the process exits after logging the time
  }
}

// Listen for abrupt exits, errors, and normal exit to log the time before the script exits
process.on('exit', () => logTimeElapsed(startTime));
process.on('SIGINT', () => logTimeElapsed(startTime)); // Catches ctrl+c event
process.on('SIGTERM', () => logTimeElapsed(startTime)); // Catches "kill pid" (POSIX)
process.on('uncaughtException', () => logTimeElapsed(startTime));

function logTimeElapsed(startTime) {
  const endTime = Date.now();
  const timeElapsed = endTime - startTime;
  console.log(`Script aborted or crashed. Total time elapsed: ${timeElapsed} ms.`);
  process.exit(1); // Exit with error code
}

// Ensure the main function is called to start the script
main().catch(err => {
  logger.error(`Script failed to start: ${err}`);
});
