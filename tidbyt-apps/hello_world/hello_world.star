load("render.star", "render")

def main():
    return render.Root(
        child = render.Box(
            render.Column(
                expanded = True,
                main_align = "center",
                cross_align = "center",
                children = [
                    render.Text(
                        content = "BCABC",
                        font = "6x13",
                        color = "#0ff",
                    ),
                    render.Box(width = 64, height = 2),
                    render.Text(
                        content = "Hello Tidbyt!",
                        font = "tom-thumb",
                        color = "#ff0",
                    ),
                ],
            ),
        ),
    )
