# Network Generator
## What it does?
The main purpose of this repository is to read files from source data and create the river network edges that represent a river connecting to another river. The original data was not inherently designed to do this. So some modifications and minor logic was needed to create this network.

## How to use this repository?
1. First clone the repository.
2. Create virtual environment to use or use an existing one
3. install requirements.txt
```shell
pip install -r requirements.txt
```
4. Run shell command
```shell
python networkGenerator --make-edges 
```

## Future of this repository
* Likely only going to be used to generate edge data which will be migrated to db
* May be expanded or utilized into commons or the backend service to regenerate edge data for new river systems
* Needs work to be able to take in data from source and convert to edge network