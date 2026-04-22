# Bilibili & Douyin Video Downloader

A Windows desktop/CLI tool for downloading videos from Bilibili and Douyin (public videos you have legal access to).

## Features

- Auto-detect platform (Bilibili/Douyin mixed links batch download)
- Pre-download preview (title/author/duration) with checkbox selection
- Embedded real-time preview (python-vlc + VLC)
- Bilibili multi-P/playlist selection
- GUI task cancellation (safe stop)
- Progress bar with speed and ETA
- Cookie support (cookies.txt or browser cookies)
- Resolution selection and audio extraction (mp3/m4a/wav/flac)
- Subtitle, thumbnail, metadata JSON export
- Download history and duplicate skipping
- Task import/export (txt/csv/json)
- Proxy, rate limit, retry settings
- Generic webpage download (Playwright + ffmpeg)
- Runtime self-check and one-click repair

## Quick Start

### GUI
Double-click `start_app.bat` and paste links.

### CLI
```powershell
python .\bilibili_downloader.py --platform auto --resolution 1080 "https://www.bilibili.com/video/BVxxxxxxxxxx"
```

## Build EXE
```powershell
.\build_exe.bat
```

## License
MIT
