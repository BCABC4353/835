load("render.star", "render")
load("http.star", "http")

# Open-Meteo: free weather API, no auth required
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

def main(config):
    # Config parameters with defaults (use config.get with fallback values)
    # Pass config values when rendering: pixlet render template.star city=Tokyo
    city = config.get("city", "London")
    text_color = config.get("text_color", "#fff")
    accent_color = config.get("accent_color", "#0af")
    lat = config.get("lat", "51.51")
    lon = config.get("lon", "-0.13")
    use_api = config.get("use_api", "false")

    # Default/demo values
    temp = "72"
    wind = "5"
    desc = "Partly cloudy"

    # Fetch live data when use_api=true
    # Usage: pixlet render template.star use_api=true lat=40.71 lon=-74.01 city=NYC
    if use_api == "true":
        resp = http.get(
            url = WEATHER_URL,
            params = {
                "latitude": lat,
                "longitude": lon,
                "current_weather": "true",
                "temperature_unit": "fahrenheit",
            },
            ttl_seconds = 600,
        )
        if resp.status_code == 200:
            data = resp.json()
            current = data["current_weather"]
            temp = str(int(current["temperature"]))
            wind = str(int(current["windspeed"]))
            desc = weather_desc(int(current["weathercode"]))

    return render.Root(
        # Delay controls animation speed (milliseconds per frame)
        delay = 75,
        child = render.Column(
            expanded = True,
            main_align = "space_between",
            children = [
                # Top row: city name with colored background box
                render.Box(
                    width = 64,
                    height = 9,
                    color = "#226",
                    child = render.Row(
                        expanded = True,
                        main_align = "center",
                        children = [
                            render.Text(
                                content = city,
                                font = "tom-thumb",
                                color = accent_color,
                            ),
                        ],
                    ),
                ),
                # Middle row: temperature in large font + wind speed
                render.Row(
                    expanded = True,
                    main_align = "center",
                    cross_align = "center",
                    children = [
                        render.Text(
                            content = temp + "F",
                            font = "6x13",
                            color = text_color,
                        ),
                        render.Box(width = 4, height = 1),
                        render.Column(
                            children = [
                                render.Text(
                                    content = wind + "mph",
                                    font = "tom-thumb",
                                    color = "#888",
                                ),
                            ],
                        ),
                    ],
                ),
                # Bottom: animated scrolling text with Marquee
                render.Box(
                    width = 64,
                    height = 8,
                    color = "#112",
                    child = render.Marquee(
                        width = 64,
                        child = render.Text(
                            content = desc,
                            font = "tom-thumb",
                            color = "#ff0",
                        ),
                    ),
                ),
            ],
        ),
    )

def weather_desc(code):
    """Map WMO weather codes to descriptions."""
    descs = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Rime fog",
        51: "Light drizzle",
        53: "Drizzle",
        55: "Dense drizzle",
        61: "Slight rain",
        63: "Rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Snow",
        75: "Heavy snow",
        80: "Rain showers",
        81: "Heavy showers",
        82: "Violent showers",
        95: "Thunderstorm",
        96: "Hail thunderstorm",
        99: "Heavy hail storm",
    }
    if code in descs:
        return descs[code]
    return "Weather code: " + str(code)
