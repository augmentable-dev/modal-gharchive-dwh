import modal

image = modal.Image.debian_slim().pip_install("duckdb")
app = modal.App("gharchive-image")


@app.function(image=image)
def notebook_image():
    pass
