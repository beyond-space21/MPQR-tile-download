import json

with open('batch_cords.json', 'r') as f:
    data = json.load(f)

print(len(data))