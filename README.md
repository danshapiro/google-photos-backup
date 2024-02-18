<div align="center">
<h1>Google Photos Backup, Stubborn Edition</h1>
</div>

# Overview

I loved the idea of <a href = https://github.com/danshapiro/google-photos-backup>google-photos-backup</a> but it had all sorts of problems on my actual google photos album. Old video files wouldn't download, causing it to hang. Some videos would appear without navigation arrows, which just left everything locked up.

Then I noticed it didn't download raw files... and there was no way to back up metadata...

The fixes piled up, and eventually, I decided to use a new approach.

## How it works

This still uses Playwright to open Google Photos. Then things get different:

- It scans the home page of photos.google.com and adds the URL of every photo/video to a SQLite db.
- It scans the \downloads directory to find all existing downloads and loads them into the SQLite db as well.
- It finds the first photo that doesn't have a corresponding file and downloads it.
- It stores the photo's gphotos URL in metadata (your choice of EXIF, XMP sidecar, or both) so later we can figure out if that URL has been backed up
- It continues until all images are backed up
- If it finds an error, it stores it in the /logs directory and keeps going
- You can seamlessly restart it if you have a problem.

## Installation

To get started with this project, follow these steps:

### Clone the repository:
```bash
git clone https://github.com/danshapiro/google-photos-backup
```

### Install dependencies:
```bash
npm install
npx playwright install --with-deps chrome
```

### Setup login session:

```bash
node setup.js
```

This will open a Chrome browser and ask you to log in to your Google Photos account. After logging in, either press ctrl+c or close the browser. This will save your login session in the `session` folder.

### Run the project:

```bash
node index.js
```

## Details you'll need:

- It's very, very slow. Lots of painstaking navigation and clicking.
- During the first scan, if it finds 10 URLs it's seen before, it stops. That allows this step to go quickly when you're doing an incremental backup (or picking up an an aborted backup).
- It downloads everything it can: the original file (without edits), the video (if it's a motion photo for example), the final image (with edits etc), and anything else it can find.
- It also stores metadata it gets from scraping the site: people's names, any descriptions that were set, etc.
- If something gets added that isn't a latest image, this won't catch it. Set dropAndCreatePhotosTable = false once and run it, then set it back. It will rescan google photos (might take a day).
- Out of my ~100k images & videos, I found perhaps a hundred that were corrupted. You can edit them, resave them as a copy, and often the copy will download. But since the copy isn't recent, it won't be picked up with a full rescan. I suggest doing this once at the end since it's pretty time consuming to do a rescan.

## Credits
This started life as [https://github.com/vikas5914/google-photos-backup](https://github.com/vikas5914/google-photos-backup) and wouldn't have been possible without it.

## License
This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
