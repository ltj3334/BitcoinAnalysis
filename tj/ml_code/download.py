import gdown
file_name = ["blockchain2020out.csv"]
file_id = ["11LFiTVYpEJCgRHiMbIS7mvx571Gkitc7"]

for name, id in zip(file_name, file_id):
    URL =f"https://drive.google.com/uc?id={id}"
    output = name
    gdown.download(URL, output, quiet=True)
    