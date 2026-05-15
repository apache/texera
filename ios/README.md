# Texera iOS

A SwiftUI iOS app that renders the existing Texera Angular frontend inside a
`WKWebView`. The web view is the same UI that runs in a desktop browser, so the
full workflow editor (JointJS, Monaco, etc.) is available; native chrome adds
back/forward, reload, and a configurable backend URL.

```
iPhone  ──▶  WKWebView  ──▶  http(s)://<texera-host>:<port>
                              (Angular SPA served by `yarn start` or a deployed
                               Texera instance)
```

## Why a wrapper, not a native rewrite?

Re-implementing the JointJS workflow canvas in SwiftUI is months of work.
Wrapping the Angular SPA gets the full editor onto iOS in days while leaving
one canonical UI to maintain. The trade-off: touch ergonomics on the canvas
will be only as good as the web app makes them. Improving those is follow-up
work in the `frontend/` module, not here.

## Requirements

| Tool | Version |
| --- | --- |
| macOS | 14 Sonoma or newer |
| Xcode | 15.0+ (16+ recommended; iOS 17.0 deployment target) |
| iOS Simulator runtime | Whatever ships with your Xcode |
| [XcodeGen](https://github.com/yonaskolb/XcodeGen) | *Only* if you add/remove files — see below |

## Run it

The `Texera.xcodeproj` is checked in. Just open it:

```bash
open ios/Texera.xcodeproj
```

Pick an iPhone simulator in the toolbar and ⌘R.

If you don't have an iOS Simulator runtime installed, Xcode will prompt you to
download one from **Settings → Components**.

## Adding or removing source files

[`project.yml`](project.yml) is the source of truth for the project structure.
After adding/renaming/deleting files under `Texera/`, regenerate the
`.xcodeproj` so it stays in sync:

```bash
brew install xcodegen   # one-time
cd ios && xcodegen generate
```

Commit the updated `Texera.xcodeproj` alongside your source changes. Editing
the pbxproj by hand or via Xcode's "Add Files…" is fine for quick local
experiments but will diverge from `project.yml` — prefer regeneration.

## Pointing the app at a backend

The default backend URL is `http://localhost:4200` (the Angular dev server).
Change it in the app via the gear icon in the bottom toolbar. The value
persists in `UserDefaults` under `backendURL`.

For development against a Mac running `yarn start` in `frontend/`:

| Where the app runs | URL to set |
| --- | --- |
| iOS Simulator on the same Mac | `http://localhost:4200` |
| Physical iPhone on the same Wi-Fi | `http://<mac-lan-ip>:4200` |
| Deployed Texera | `https://<your-host>` |

`http://localhost` and local-network IPs are allowed via an ATS exception in
[Texera/Info.plist](Texera/Info.plist). Public HTTPS works without
configuration. Plain HTTP to public hosts is intentionally blocked.

## What's in the box

| File | Role |
| --- | --- |
| [project.yml](project.yml) | XcodeGen spec — single source of truth for the project |
| [Texera/TexeraApp.swift](Texera/TexeraApp.swift) | `@main` entry point |
| [Texera/ContentView.swift](Texera/ContentView.swift) | Root view: web view + bottom toolbar |
| [Texera/WebView.swift](Texera/WebView.swift) | `UIViewRepresentable` wrapping `WKWebView` |
| [Texera/SettingsView.swift](Texera/SettingsView.swift) | Sheet to edit the backend URL |
| [Texera/Info.plist](Texera/Info.plist) | Usage descriptions, ATS exceptions |
| [Texera/Assets.xcassets](Texera/Assets.xcassets) | App icon and accent color placeholders |

## Known gaps (deliberate)

- **App icon is blank.** Drop a 1024×1024 PNG into
  `Texera/Assets.xcassets/AppIcon.appiconset/` and update its `Contents.json`.
- **No native login flow.** Auth is whatever the wrapped web app does.
- **No push notifications.** Could be added by registering for APNs and
  posting from a backend service; out of scope for this scaffold.
- **No offline cache beyond what `WKWebView` does on its own.**
- **No Android.** This is iOS-only by design; a parallel `android/` folder
  with a `WebView` activity is a separate effort.

## Tests

This scaffold has no tests yet — the only logic is the web view wrapper. When
adding behaviour (URL validation, JS↔native bridge, offline handling), add an
`XCTest` target via `project.yml` and follow the TDD rule in
[../AGENTS.md](../AGENTS.md) ("Tests come first").
