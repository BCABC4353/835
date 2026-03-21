# Tidbyt Apps

Custom applets for the Tidbyt 64x32 RGB LED display.

## Setup

1. Install pixlet: download from https://github.com/tidbyt/pixlet/releases
2. Edit `.env` and add your `TIDBYT_DEVICE_ID` and `TIDBYT_API_TOKEN` (find both in the Tidbyt mobile app under Settings → your device → API)

## Development Workflow

### Preview locally
```bash
pixlet serve my_applet.star
# Open http://localhost:8080 in your browser
```

### Render to image
```bash
pixlet render my_applet.star
# Creates my_applet.webp
```

### Push to device (one-time)
```bash
./render-and-push.sh my_applet.star
```

### Push to device (stays in rotation)
```bash
./render-and-push-persistent.sh my_applet.star
# Uses filename as installation ID by default, or specify one:
./render-and-push-persistent.sh my_applet.star my-custom-id
```

## Creating a New Applet

1. Create a directory `my_applet/` with `my_applet.star` inside (pixlet loads all .star files in a directory, so each applet needs its own folder)
2. Every applet needs a `main()` function that returns a `render.Root` widget
3. The display is 64 pixels wide × 32 pixels tall
4. Preview with `cd my_applet && pixlet serve my_applet.star`
5. Push with `./render-and-push.sh my_applet/my_applet.star`

## Key Concepts

- **render.Root**: Top-level container (required)
- **render.Column / render.Row**: Vertical/horizontal layouts
- **render.Text**: Display text (fonts: `tb-8`, `tom-thumb`, `6x13`, `10x20`)
- **render.Box**: Colored rectangles, useful for backgrounds
- **render.Marquee**: Scrolling/animated text
- **render.Image**: Display images (base64 encoded)
- **config.get()**: Read config parameters with defaults
- **http.get()**: Fetch data from APIs (cached automatically)

## Files

- `hello_world/hello_world.star` — Simple test applet displaying "BCABC"
- `template/template.star` — Full-featured template with API calls, layout, animation, and config
- `render-and-push.sh` — One-time push helper
- `render-and-push-persistent.sh` — Persistent rotation push helper
